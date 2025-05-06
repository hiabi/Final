# Versión final corregida con retry automático y selector de algoritmo
import streamlit as st
import pandas as pd
import numpy as np
import random
import networkx as nx
import datetime
import time
from io import BytesIO
from pymongo import MongoClient
import uuid

@st.cache_resource
def get_mongo_collection():
    client = MongoClient(st.secrets["mongo"]["uri"])
    db = client.car_exchange
    collection = db.user_uploads
    return collection

mongo_collection = get_mongo_collection() if "mongo" in st.secrets else None

def load_offer_want_excel(file):
    xls = pd.ExcelFile(file)
    offers = pd.read_excel(xls, 'Offers')
    wants = pd.read_excel(xls, 'Wants')
    return offers.to_dict('records'), wants.to_dict('records')

def save_user_data_to_mongo(offers, wants, name, agency_id):
    mongo_collection.update_one(
        {"agency_id": agency_id},
        {
            "$push": {
                "uploads": {
                    "offers": offers,
                    "wants": wants,
                    "uploaded_at": datetime.datetime.now()
                }
            },
            "$setOnInsert": {
                "user_id": str(uuid.uuid4()),
                "name": name,
                "agency_id": agency_id
            }
        },
        upsert=True
    )

def load_all_requests_from_mongo():
    requests = []
    participants = list(mongo_collection.find({}))
    for user in participants:
        for upload in user.get("uploads", []):
            offers = upload.get('offers', [])
            wants = upload.get('wants', [])
            for offer in offers:
                if 'full_name' not in offer and 'MODELO' in offer and 'VERSION' in offer:
                    offer['full_name'] = offer['MODELO'].strip().upper() + " - " + offer['VERSION'].strip().upper()
            for want in wants:
                if 'full_name' not in want and 'MODELO' in want and 'VERSION' in want:
                    want['full_name'] = want['MODELO'].strip().upper() + " - " + want['VERSION'].strip().upper()

            requests.append({
                'id': user['agency_id'],
                'name': user.get('name', user['agency_id']),
                'offers': offers,
                'wants': wants,
                'created_at': upload.get('uploaded_at', datetime.datetime.now()),
                'status': 'pending'
            })
    return requests

def build_graph(requests):
    G = nx.DiGraph()
    for req in requests:
        G.add_node(req['id'])

    for req_a in requests:
        for req_b in requests:
            if req_a['id'] == req_b['id']:
                continue
            if any(o['full_name'].lower() == w['full_name'].lower() for o in req_a['offers'] for w in req_b['wants']):
                G.add_edge(req_a['id'], req_b['id'])
    return G

def violates_offer_conflict(cycle, request_map, used_offers):
    for i in range(len(cycle) - 1):
        giver_id = cycle[i]
        receiver_id = cycle[i + 1]
        giver = request_map[giver_id]
        receiver = request_map[receiver_id]
        for offer in giver['offers']:
            for want in receiver['wants']:
                if offer['full_name'].lower() == want['full_name'].lower():
                    key = (giver_id, offer['full_name'])
                    if key in used_offers:
                        return True
                    used_offers.add(key)
    return False

def sample_cycles_greedy(G, request_map, max_len=10):
    all_cycles = []
    used_nodes = set()
    used_offers = set()

    for component in nx.connected_components(G.to_undirected()):
        subgraph = G.subgraph(component).copy()
        cycles = list(nx.simple_cycles(subgraph))
        cycles = [c for c in cycles if len(c) >= 3 and c[0] == c[-1]]
        cycles.sort(key=len, reverse=True)
        for cycle in cycles:
            if not any(node in used_nodes for node in cycle):
                if not violates_offer_conflict(cycle, request_map, used_offers):
                    all_cycles.append(cycle)
                    used_nodes.update(cycle)
    return all_cycles

def sample_cycles_exhaustive(G, request_map, max_len=10):
    all_cycles = []
    used_nodes = set()
    used_offers = set()

    for component in nx.connected_components(G.to_undirected()):
        subgraph = G.subgraph(component).copy()
        for start in subgraph.nodes:
            stack = [(start, [start])]
            while stack:
                node, path = stack.pop()
                for neighbor in subgraph.successors(node):
                    if neighbor == start and len(path) >= 3:
                        cycle = path + [start]
                        if not any(p in used_nodes for p in cycle):
                            if not violates_offer_conflict(cycle, request_map, used_offers):
                                all_cycles.append(cycle)
                                used_nodes.update(cycle)
                        break
                    elif neighbor not in path and len(path) < max_len:
                        stack.append((neighbor, path + [neighbor]))
    return all_cycles

def describe_cycles(cycles, request_map):
    all_cycles = []
    user_cycles = []

    for cycle_id, cycle in enumerate(cycles):
        if len(cycle) < 3 or cycle[0] != cycle[-1]:
            continue

        description = []
        for i in range(len(cycle) - 1):
            giver_id = cycle[i]
            receiver_id = cycle[i + 1]
            giver = request_map[giver_id]
            receiver = request_map[receiver_id]
            matching_offer = next((o for o in giver['offers'] for w in receiver['wants']
                                   if o['full_name'].lower() == w['full_name'].lower()), None)
            if matching_offer:
                line = f"{giver['name']} offers '{matching_offer['full_name']}' → to {receiver['name']}"
                description.append(line)

        exchange_text = "\n".join(description)
        all_cycles.append({'cycle_id': cycle_id, 'exchange_path': exchange_text})

        if 0 in cycle:
            user_cycles.append({'cycle_id': cycle_id, 'exchange_path': exchange_text})

    return pd.DataFrame(all_cycles), pd.DataFrame(user_cycles)

st.title("🚗 Car Exchange Platform")

if mongo_collection is None:
    st.error("MongoDB not connected.")
    st.stop()

st.header("📤 Upload Your Offers and Wants")
name = st.text_input("Enter your Name")
agency_id = st.text_input("Enter your Agency ID")
user_file = st.file_uploader("Upload Excel file (Offers/Wants):", type=["xlsx"])

if st.button("Upload File"):
    if not name.strip() or not agency_id.strip():
        st.error("Please fill Name and Agency ID.")
    elif not user_file:
        st.error("Please select a file to upload.")
    else:
        offers, wants = load_offer_want_excel(user_file)
        save_user_data_to_mongo(offers, wants, name, agency_id)
        st.success(f"Uploaded {len(offers)} offers and {len(wants)} wants.")
        st.balloons()

st.markdown("---")
st.header("🔄 Run Matching Across All Current Uploads")

algo_choice = st.radio("Choose Cycle Detection Algorithm:", ["Greedy (Efficient)", "Exhaustive (Comprehensive)"])

if st.button("🧮 Find Exchange Cycles"):
    all_requests = load_all_requests_from_mongo()
    if not all_requests:
        time.sleep(10)
        all_requests = load_all_requests_from_mongo()

    request_map = {r['id']: r for r in all_requests}
    G = build_graph(all_requests)

    if algo_choice == "Greedy (Efficient)":
        cycles = sample_cycles_greedy(G, request_map)
    else:
        cycles = sample_cycles_exhaustive(G, request_map)

    df_all, _ = describe_cycles(cycles, request_map)

    st.subheader("🔍 Exchange Cycles Preview")
    st.dataframe(df_all.head(10))

    output = BytesIO()
    df_all.to_csv(output, index=False)
    st.download_button("📥 Download All Cycles", data=output.getvalue(), file_name="exchange_cycles.csv", mime="text/csv")

st.markdown("---")
with st.expander("⚠️ Danger Zone - Admin Only"):
    password = st.text_input("Admin Password to Reset:", type="password")
    if st.button("🗑️ Clear All Uploads"):
        if password == "050699":
            mongo_collection.delete_many({})
            st.warning("All uploads have been cleared.")
        else:
            st.error("Incorrect password.")

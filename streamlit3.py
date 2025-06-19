import streamlit as st
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime
import math
import io

# === Config ===
API_BASE_URL = "https://oms.locus-api.com/v1/client/watsons-ph-devo/order/"
USERNAME = "watsons-ph-devo"
PASSWORD = "977eb974-6fcd-4678-97e6-191f543b1a04"

# === Functions ===

def parse_csv(file) -> dict:
    try:
        df = pd.read_csv(file, delimiter='\t')
        df.columns = [col.strip() for col in df.columns]
        if 'Order ID' not in df.columns:
            raise ValueError("Missing 'Order ID' in tab-delimited file.")
    except Exception:
        file.seek(0)
        df = pd.read_csv(file)
        df.columns = [col.strip() for col in df.columns]
        if 'Order ID' not in df.columns:
            raise ValueError(f"'Order ID' not found. Found columns: {df.columns.tolist()}")

    orders = defaultdict(list)
    for row in df.to_dict(orient="records"):
        orders[row['Order ID']].append(row)
    return dict(orders)


def build_payload(order_rows):
    first = order_rows[0]
    total_volume = sum(float(row['Volume']) for row in order_rows)

    payload = {
        "type": first["Type"],
        "teamId": first["Team ID"],
        "volume": {
            "value": str(total_volume),
            "unit": first["Volume Unit"]
        },
        "lineItems": [],
        "homebaseId": first["Homebase ID"],
        "locationId": first["Location ID"],
        "date": datetime.strptime(first["Customer Execution Date"], "%d/%m/%Y").strftime("%Y-%m-%d"),
        "orderDate": datetime.strptime(first["Customer Execution Date"], "%d/%m/%Y").strftime("%Y-%m-%d")
    }

    for row in order_rows:
        line_item = {
            "id": row["Sku Line Item ID"],
            "lineItemId": row["Sku Line Item ID"],
            "name": row["Case"],
            "category": row["Category"],
            "handlingUnit": "QUANTITY",
            "quantity": int(row["Quantity"]),
            "quantityUnit": row["Quantity Unit"],
            "parts": [{
                "volume": {
                    "value": row["Volume"],
                    "unit": row["Volume Unit"]
                }
            }]
        }
        payload["lineItems"].append(line_item)

    return payload


def send_order(order_id, payload):
    url = f"{API_BASE_URL}{order_id}"
    try:
        response = requests.put(
            url,
            json=payload,
            auth=(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"}
        )
        return response.status_code, response.text
    except requests.RequestException as e:
        return None, str(e)


def batch_orders(order_dict, batch_size):
    order_ids = list(order_dict.keys())
    total_batches = math.ceil(len(order_ids) / batch_size)
    batches = []

    for i in range(total_batches):
        start = i * batch_size
        end = start + batch_size
        batch_ids = order_ids[start:end]
        batch = {oid: order_dict[oid] for oid in batch_ids}
        batches.append(batch)

    return batches


# === Streamlit App ===

st.set_page_config(page_title="Locus Order Uploader", page_icon="📦")
st.title("📦 Locus Order Uploader")

uploaded_file = st.file_uploader("Upload your order CSV", type=["csv", "tsv"])

batch_size = st.number_input(
    "Set batch size (orders per batch)", min_value=1, max_value=1000, value=100, step=1
)

st.divider()

if uploaded_file:
    try:
        all_orders = parse_csv(uploaded_file)
        total_orders = len(all_orders)
        st.success(f"✅ Found {total_orders} unique order(s).")

        batches = batch_orders(all_orders, batch_size)

        st.info(f"Orders are split into {len(batches)} batch(es) of up to {batch_size} orders each.")

        # To hold failed orders for download, keyed by batch number
        if "failed_orders_batches" not in st.session_state:
            st.session_state.failed_orders_batches = {}

        for i, batch in enumerate(batches):
            with st.expander(f"Batch {i+1}: {len(batch)} orders", expanded=False):
                btn_key = f"send_batch_{i}"
                if st.button(f"🚀 Send Batch {i+1}", key=btn_key):
                    failed_orders = []
                    with st.spinner(f"Sending Batch {i+1}..."):
                        for order_id, rows in batch.items():
                            payload = build_payload(rows)
                            status, response = send_order(order_id, payload)
                            if status and status < 300:
                                st.success(f"✅ Sent Order ID {order_id} — Status {status}")
                            else:
                                st.error(f"❌ Failed Order ID {order_id} — {response}")
                                # Collect failed orders' rows for download
                                failed_orders.extend(rows)

                    if failed_orders:
                        # Save failed orders in session_state keyed by batch
                        st.session_state.failed_orders_batches[i] = failed_orders

            # After the batch expander, show download button if failures for that batch exist
            if i in st.session_state.failed_orders_batches:
                failed_df = pd.DataFrame(st.session_state.failed_orders_batches[i])
                csv_buffer = io.StringIO()
                failed_df.to_csv(csv_buffer, index=False)
                csv_bytes = csv_buffer.getvalue().encode()
                dl_button_key = f"download_failed_batch_{i}"
                st.download_button(
                    label=f"⬇️ Download Failed Orders from Batch {i+1}",
                    data=csv_bytes,
                    file_name=f"failed_orders_batch_{i+1}.csv",
                    mime="text/csv",
                    key=dl_button_key
                )

    except Exception as e:
        st.error(f"❌ Error processing file: {e}")

import pandas as pd
import os
import shutil

# --- CONFIGURATION ---
# Adjust this path if your script is in a different folder
# We assume script is in 'scripts/' and data is in 'data/'
SOURCE_FILE = "../data/fraud_data.csv"
OUTPUT_BASE = "../data"


def setup_folders():
    """Cleans and recreates the destination folders to ensure a fresh start."""
    folders = ["source_transactions", "source_account_info", "source_fraud_labels"]
    for folder in folders:
        path = os.path.join(OUTPUT_BASE, folder)
        if os.path.exists(path):
            shutil.rmtree(path)  # Safety wipe
        os.makedirs(path)
        print(f"Created/Cleaned folder: {path}")


def run_simulation():
    print("--- STARTING SIMULATION ---")
    setup_folders()

    # 1. READ
    print(f"Reading {SOURCE_FILE}... (This may take 30-60 seconds for 6M rows)")
    df = pd.read_csv(SOURCE_FILE)
    total_rows_original = len(df)
    print(f"Original Row Count: {total_rows_original:,}")

    # 2. GENERATE KEYS
    # We create a simple unique ID.
    # Format: TXN-{step}-{index}. This is safer and faster than UUID for this simulation.
    print("Generating Transaction IDs...")
    df['transaction_id'] = "TXN-" + df['step'].astype(str) + "-" + df.index.astype(str)

    # 3. SPLIT & SAVE BY STEP
    # We group by 'step' (Hour) to mimic hourly batch ingestion
    grouped = df.groupby('step')

    total_rows_written = 0
    total_batches = 0

    print("Splitting data into 3 systems... (This will take a few minutes)")

    for step, group in grouped:
        # Define the splits explicitly

        # SYSTEM A: Transactions (The Core)
        # Renaming columns to mimic a cleaner database schema
        df_trans = group[['transaction_id', 'step', 'type', 'amount', 'nameOrig', 'nameDest']].copy()
        df_trans.rename(columns={'nameOrig': 'origin_account_id', 'nameDest': 'dest_account_id'}, inplace=True)

        # SYSTEM B: Account Info (The Balances)
        df_acct = group[
            ['transaction_id', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']].copy()

        # SYSTEM C: Fraud Labels (The Risk Engine)
        df_fraud = group[['transaction_id', 'isFraud', 'isFlaggedFraud']].copy()

        # Save to CSVs
        # We assume the code is running from 'scripts/' so we go up one level
        df_trans.to_csv(f"{OUTPUT_BASE}/source_transactions/batch_step_{step}.csv", index=False)
        df_acct.to_csv(f"{OUTPUT_BASE}/source_account_info/batch_step_{step}.csv", index=False)
        df_fraud.to_csv(f"{OUTPUT_BASE}/source_fraud_labels/batch_step_{step}.csv", index=False)

        total_rows_written += len(group)
        total_batches += 1

        # Progress indicator every 50 steps (hours)
        if step % 50 == 0:
            print(f"Processed up to Step {step}...")

    # --- AUDIT REPORT ---
    print("\n--- FINAL AUDIT ---")
    print(f"Original Input Rows: {total_rows_original:,}")
    print(f"Total Output Rows:   {total_rows_written:,}")
    print(f"Total Batches/Files: {total_batches}")

    if total_rows_original == total_rows_written:
        print("✅ SUCCESS: No Data Loss detected.")
    else:
        print(f"❌ ERROR: Data Loss! Missing {total_rows_original - total_rows_written} rows.")


if __name__ == "__main__":
    run_simulation()
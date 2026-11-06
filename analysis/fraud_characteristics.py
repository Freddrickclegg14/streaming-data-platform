import pandas as pd

# df = pd.read_csv("data/card_transaction.v1.csv")
# columns_list = df.columns.tolist()

# print("Columns in the dataset:")
# for col in columns_list:
#     print(f" - {col}")

df = pd.read_csv("data/card_transaction.v1.csv")
df["amount_clean"] = df["Amount"].str.replace("$", "", regex=False).astype(float)
df["is_fraud"] = df["Is Fraud?"].str.strip().str.lower() == "yes"

fraud = df[df["is_fraud"]]
legit = df[~df["is_fraud"]]

print("=== Amount ====")
print(
    "Fraud mean/median: ", fraud["amount_clean"].mean(), fraud["amount_clean"].median()
)
print(
    "Legit mean/median: ", legit["amount_clean"].mean(), legit["amount_clean"].median()
)

print("\n=== Use chip ====")
print(fraud["Use Chip"].value_counts(normalize=True))
print(legit["Use Chip"].value_counts(normalize=True))

print("\n=== TOP MCCs in Fraud ====")
print(fraud["MCC"].value_counts(normalize=True).head(10))

print("\n=== online (no merchant state) rate ====")
print("Fraud online rate: ", fraud["Merchant State"].isna().mean())
print("Legit online rate: ", legit["Merchant State"].isna().mean())

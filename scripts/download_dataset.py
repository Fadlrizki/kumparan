from huggingface_hub import hf_hub_download

path = hf_hub_download(
    repo_id="iqballx/indonesian_news_datasets",
    filename="data.csv",          # BUKAN data_with_nomic_embeddings.csv
    repo_type="dataset",
    local_dir="data/raw",
)
print("Saved to:", path)
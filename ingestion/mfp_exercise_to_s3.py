import pandas as pd
import boto3
from pathlib import Path
import tempfile

DATA_FOLDER = Path("myfitnesspal")
BUCKET = "fitness-data-pipeline"
PREFIX = "mfp/exercise"


def get_latest_file():
    files = list(DATA_FOLDER.glob("*Exercise-Summary*.csv"))
    if not files:
        raise FileNotFoundError(f"No exercise CSV files found in {DATA_FOLDER.resolve()}")
    return max(files, key=lambda f: f.stat().st_mtime)


def clean_dataframe(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    return df


def upload_to_s3(file_path: Path, s3_filename: str):
    s3 = boto3.client("s3")
    key = f"{PREFIX}/{s3_filename}"
    s3.upload_file(str(file_path), BUCKET, key)
    print(f"Uploaded to s3://{BUCKET}/{key}")


def main():
    latest_file = get_latest_file()
    print(f"Processing {latest_file}")

    df = pd.read_csv(latest_file)
    df = clean_dataframe(df)

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as tmp:
            temp_path = Path(tmp.name)
            df.to_csv(tmp.name, index=False)

        upload_to_s3(temp_path, latest_file.name)

        # delete original local export after successful upload
        latest_file.unlink()
        print(f"Deleted original local file: {latest_file}")

    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()
            print(f"Deleted temporary file: {temp_path}")


if __name__ == "__main__":
    main()
import argparse
from pathlib import Path

DEFAULT_DATASET = "berkayalan/retail-sales-data"


def download_dataset(dataset: str, dest_dir: str | Path, force: bool = False) -> Path:
    """Telecharge et decompresse un dataset Kaggle dans `dest_dir`.

    Si des CSV sont deja presents dans `dest_dir`, le telechargement est skip.
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    if not force and any(dest.glob("*.csv")):
        print(f"Dataset deja present dans {dest}, telechargement ignore (--force pour forcer).")
        return dest

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path=str(dest), unzip=True, quiet=False)

    return dest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Slug du dataset Kaggle.")
    parser.add_argument(
        "--dest",
        default="data/raw/retail_sales",
        help="Dossier de destination (cree si besoin).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Retelecharge meme si des CSV sont deja presents dans --dest.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    dest = download_dataset(args.dataset, args.dest, force=args.force)
    print(f"Dataset '{args.dataset}' disponible dans {dest}")


if __name__ == "__main__":
    main()

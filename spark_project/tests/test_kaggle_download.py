from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from ingestion.kaggle_download import download_dataset


def _install_fake_kaggle_api(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Remplace le package `kaggle` par un faux module dans sys.modules.

    Evite de dependre du vrai package `kaggle` (qui verifie des identifiants
    des l'import) pour tester notre logique de telechargement.
    """
    fake_api_instance = MagicMock()

    extended_module = ModuleType("kaggle.api.kaggle_api_extended")
    extended_module.KaggleApi = MagicMock(return_value=fake_api_instance)  # type: ignore[attr-defined]

    api_module = ModuleType("kaggle.api")
    api_module.kaggle_api_extended = extended_module  # type: ignore[attr-defined]

    kaggle_module = ModuleType("kaggle")
    kaggle_module.api = api_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "kaggle", kaggle_module)
    monkeypatch.setitem(sys.modules, "kaggle.api", api_module)
    monkeypatch.setitem(sys.modules, "kaggle.api.kaggle_api_extended", extended_module)

    return fake_api_instance


def test_download_dataset_calls_kaggle_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_api = _install_fake_kaggle_api(monkeypatch)
    dest_dir = tmp_path / "out"

    result = download_dataset("someuser/some-dataset", dest_dir)

    assert result == dest_dir
    fake_api.authenticate.assert_called_once()
    fake_api.dataset_download_files.assert_called_once_with(
        "someuser/some-dataset", path=str(dest_dir), unzip=True, quiet=False
    )


def test_download_dataset_skips_if_csv_already_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    (dest_dir / "Reviews.csv").write_text("Id,ProductId\n1,B001\n")

    fake_api = _install_fake_kaggle_api(monkeypatch)

    download_dataset("someuser/some-dataset", dest_dir)

    fake_api.dataset_download_files.assert_not_called()


def test_download_dataset_force_redownloads_even_if_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    (dest_dir / "Reviews.csv").write_text("Id,ProductId\n1,B001\n")

    fake_api = _install_fake_kaggle_api(monkeypatch)

    download_dataset("someuser/some-dataset", dest_dir, force=True)

    fake_api.dataset_download_files.assert_called_once()

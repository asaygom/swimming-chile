import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from architecture_boundaries import find_architecture_violations


def write_module(backend_root: Path, relative_path: str, source: str) -> None:
    module_path = backend_root / relative_path
    module_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text(source, encoding="utf-8")


def test_detector_reports_forbidden_import_with_file_import_and_rule(tmp_path):
    backend_root = tmp_path / "backend"
    write_module(
        backend_root,
        "api/handler.py",
        "from scripts import load_results\n",
    )

    violations = find_architecture_violations(backend_root)

    assert len(violations) == 1
    assert "api/handler.py" in violations[0]
    assert "from scripts import load_results" in violations[0]
    assert "backend/api cannot import scripts" in violations[0]


def test_detector_enforces_all_boundaries_and_allows_valid_imports(tmp_path):
    backend_root = tmp_path / "backend"
    write_module(
        backend_root,
        "natacion_chile/domain/model.py",
        "\ufeffimport re\nfrom natacion_chile.domain import normalization\n",
    )
    write_module(
        backend_root,
        "natacion_chile/domain/service.py",
        "import psycopg\n",
    )
    write_module(
        backend_root,
        "natacion_chile/manifest.py",
        "import api.database\nfrom scripts import load_results\n",
    )
    write_module(
        backend_root,
        "api/handler.py",
        "from scripts import load_results\n",
    )
    write_module(
        backend_root,
        "scripts/export.py",
        "from api import database\n",
    )

    violations = find_architecture_violations(backend_root)

    assert len(violations) == 5
    assert any(
        "domain/service.py" in violation
        and "domain only imports stdlib or natacion_chile.domain" in violation
        for violation in violations
    )
    assert sum(
        "backend/natacion_chile cannot import api or scripts" in violation
        for violation in violations
    ) == 2
    assert any(
        "backend/api cannot import scripts" in violation
        for violation in violations
    )
    assert any(
        "backend/scripts cannot import api" in violation
        for violation in violations
    )


def test_repository_respects_architecture_boundaries():
    backend_root = Path(__file__).resolve().parents[1]

    violations = find_architecture_violations(backend_root)

    assert violations == [], "\n".join(violations)

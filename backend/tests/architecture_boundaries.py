import ast
import sys
from pathlib import Path


DOMAIN_RULE = (
    "backend/natacion_chile/domain only imports stdlib or "
    "natacion_chile.domain"
)
NATACION_CHILE_RULE = "backend/natacion_chile cannot import api or scripts"
API_RULE = "backend/api cannot import scripts"
SCRIPTS_RULE = "backend/scripts cannot import api"


def _matches_package(module: str, package: str) -> bool:
    return module == package or module.startswith(f"{package}.")


def _package_for_file(backend_root: Path, source_path: Path) -> list[str]:
    module_parts = list(source_path.relative_to(backend_root).with_suffix("").parts)
    module_parts.pop()
    return module_parts


def _resolve_from_module(
    backend_root: Path,
    source_path: Path,
    node: ast.ImportFrom,
) -> str:
    if node.level == 0:
        return node.module or ""

    package_parts = _package_for_file(backend_root, source_path)
    keep_count = max(0, len(package_parts) - (node.level - 1))
    resolved_parts = package_parts[:keep_count]
    if node.module:
        resolved_parts.extend(node.module.split("."))
    return ".".join(resolved_parts)


def _imports(
    backend_root: Path,
    source_path: Path,
) -> list[tuple[str, str, int]]:
    tree = ast.parse(
        source_path.read_text(encoding="utf-8-sig"),
        filename=str(source_path),
    )
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, f"import {alias.name}", node.lineno))
        elif isinstance(node, ast.ImportFrom):
            base_module = _resolve_from_module(backend_root, source_path, node)
            prefix = "." * node.level
            source_module = f"{prefix}{node.module or ''}"
            imported_names = ", ".join(alias.name for alias in node.names)
            display = f"from {source_module} import {imported_names}"
            for alias in node.names:
                target = ".".join(part for part in (base_module, alias.name) if part)
                imports.append((target, display, node.lineno))
    return imports


def _rules_for_file(relative_path: Path) -> list[tuple[str, tuple[str, ...]]]:
    path_parts = relative_path.parts
    rules = []
    if path_parts[:2] == ("natacion_chile", "domain"):
        rules.append((DOMAIN_RULE, ()))
    if path_parts[:1] == ("natacion_chile",):
        rules.append((NATACION_CHILE_RULE, ("api", "scripts")))
    elif path_parts[:1] == ("api",):
        rules.append((API_RULE, ("scripts",)))
    elif path_parts[:1] == ("scripts",):
        rules.append((SCRIPTS_RULE, ("api",)))
    return rules


def find_architecture_violations(backend_root: Path) -> list[str]:
    violations = []
    source_paths = []
    for package in ("natacion_chile", "api", "scripts"):
        package_root = backend_root / package
        if package_root.exists():
            source_paths.extend(package_root.rglob("*.py"))

    for source_path in sorted(set(source_paths)):
        relative_path = source_path.relative_to(backend_root)
        rules = _rules_for_file(relative_path)
        for module, import_display, line_number in _imports(
            backend_root,
            source_path,
        ):
            for rule, forbidden_packages in rules:
                if rule == DOMAIN_RULE:
                    root_module = module.split(".", 1)[0]
                    allowed = (
                        root_module in sys.stdlib_module_names
                        or _matches_package(module, "natacion_chile.domain")
                    )
                    forbidden = not allowed
                else:
                    forbidden = any(
                        _matches_package(module, package)
                        for package in forbidden_packages
                    )
                if forbidden:
                    violations.append(
                        f"{relative_path.as_posix()}:{line_number}: "
                        f"{import_display} violates {rule}"
                    )
    return violations

#!/usr/bin/env python3
"""
Transform the legacy per-method test-configs into the single unified-method tree.

Reads:  test-configs/        (originals; NEVER modified)
Writes: test-configs-new/    (transformed copy)

What it does
------------
1. Selects only the former PM_3 configs (``*_pm3.cfg``, which live under PBMR3/).
   These become the basis for the unified method. PBMR1/2/4 (pm1/pm2/pm4) and the
   baseline configs are intentionally dropped.
2. Flattens the directory tree: the PBMR3/ level is removed and the filename suffix
   ``_pm3`` becomes ``_unified``:
     test-configs/setX/<city|factory>/PBMR3/<name>_pm3.cfg
       -> test-configs-new/setX/<city|factory>/<name>_unified.cfg
   The internal ``name:`` field's ``_pm3`` suffix is renamed to ``_unified`` to match.
3. Rewrites the operation vocabulary in the c*_ops lists:
     Informed-Reg  -> REGISTER-INFO
     Access        -> AUDIT
     Portability   -> HISTORY
     Rectification -> UPDATE
     Erasure       -> DELETE
     Restriction   -> RESTRICT
     Informed, Object, AutoDecision -> dropped
   A list emptied by the drops (c1_ops, which only ever held Informed) becomes
   ``cN_ops: []`` so the parser still iterates an (empty) list.
4. Applies the same vocabulary rewrite to the two *_template.cfg files (copied to
   test-configs-new/ root), and additionally makes the templates a correct canonical
   source for the unified method:
     - ``purpose_management_method`` is forced to ``3`` (the only value the parser accepts).
     - the now-unused ``reg_by_msg_reg_topic`` and ``reg_by_topic_sub_reg_topic`` lines
       are dropped; only ``reg_by_topic_pub_reg_topic`` (the MP-registration topic the
       unified method actually uses) is kept.
   The data configs are left with their existing reg-topic keys (the parser ignores the
   unused ones); only the hand-maintained templates are cleaned up.

``purpose_management_method: 3`` is kept as the canonical value. No other content changes.
This script only ever writes under test-configs-new/; it does not modify originals,
swap directories, archive results, or touch the orchestration scripts.
"""

from pathlib import Path
import re

SRC = Path("test-configs")
DST = Path("test-configs-new")

RENAME = {
    "Informed-Reg":  "REGISTER-INFO",
    "Access":        "AUDIT",
    "Portability":   "HISTORY",
    "Rectification": "UPDATE",
    "Erasure":       "DELETE",
    "Restriction":   "RESTRICT",
}
DROP = {"Informed", "Object", "AutoDecision"}

HEADER_RE = re.compile(r'^(\s*)(c1_reg_ops|c1_ops|c2_ops|c3_ops):\s*$')
ITEM_RE = re.compile(r'^(\s*)-\s*"([^"]*)"\s*$')
NAME_RE = re.compile(r'^(\s*name:\s*\S*?)_pm3(\s*)$')

# Template-only cleanup: force the method and drop the reg topics the unified method doesn't use.
PMM_RE = re.compile(r'^(\s*purpose_management_method:\s*)\d+(\s*)$')
DROP_KEY_RE = re.compile(r'^\s*(reg_by_msg_reg_topic|reg_by_topic_sub_reg_topic):')


def transform_text(text: str) -> str:
    lines = text.splitlines(keepends=True)
    out = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        bare = line.rstrip("\n")
        nl = "\n" if line.endswith("\n") else ""

        # Internal test name: rename trailing _pm3 -> _unified
        name_match = NAME_RE.match(bare)
        if name_match:
            out.append(f"{name_match.group(1)}_unified{name_match.group(2)}{nl}")
            i += 1
            continue

        # Operation lists: rename / drop items, collapse emptied lists to "[]"
        header = HEADER_RE.match(bare)
        if header:
            indent, key = header.group(1), header.group(2)
            j = i + 1
            items, item_indent = [], None
            while j < n:
                m = ITEM_RE.match(lines[j].rstrip("\n"))
                if not m:
                    break
                if item_indent is None:
                    item_indent = m.group(1)
                items.append(m.group(2))
                j += 1
            new_items = [RENAME.get(op, op) for op in items if op not in DROP]
            if new_items:
                out.append(line)  # header unchanged
                ind = item_indent or (indent + "    ")
                out.extend(f'{ind}- "{op}"\n' for op in new_items)
            else:
                out.append(f"{indent}{key}: []{nl}")
            i = j
            continue

        out.append(line)
        i += 1
    return "".join(out)


def transform_template_text(text: str) -> str:
    """Op-vocab rewrite plus template-only cleanup (force method 3, drop unused reg topics)."""
    text = transform_text(text)
    out = []
    for line in text.splitlines(keepends=True):
        bare = line.rstrip("\n")
        nl = "\n" if line.endswith("\n") else ""

        if DROP_KEY_RE.match(bare):
            continue  # drop reg_by_msg_reg_topic / reg_by_topic_sub_reg_topic

        m = PMM_RE.match(bare)
        if m:
            out.append(f"{m.group(1)}3{m.group(2)}{nl}")
            continue

        out.append(line)
    return "".join(out)


def out_path_for_config(src_path: Path) -> Path:
    """Drop the PBMR3 path component and rename _pm3 -> _unified."""
    rel_parts = [p for p in src_path.relative_to(SRC).parts if p != "PBMR3"]
    rel = Path(*rel_parts)
    return DST / rel.with_name(rel.name.replace("_pm3.cfg", "_unified.cfg"))


def main():
    configs = sorted(SRC.rglob("*_pm3.cfg"))
    templates = sorted(SRC.glob("*_template.cfg"))

    for src in configs:
        dst = out_path_for_config(src)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(transform_text(src.read_text(encoding="utf-8")), encoding="utf-8")

    for tmpl in templates:
        dst = DST / tmpl.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(transform_template_text(tmpl.read_text(encoding="utf-8")), encoding="utf-8")

    print(f"Wrote {len(configs)} unified configs + {len(templates)} templates to {DST}/")


if __name__ == "__main__":
    main()

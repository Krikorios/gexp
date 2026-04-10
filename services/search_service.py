from database.connection import get_db


def normalize_arabic(text: str) -> str:
    """Normalize Arabic text for search: collapse alef variants, ta marbuta, alef maqsura."""
    if not text:
        return text
    for ch in "أإآ":
        text = text.replace(ch, "ا")
    text = text.replace("ة", "ه")
    text = text.replace("ى", "ي")
    return text


def search_persons(query: str) -> list[dict]:
    """Search persons by name (first, family, or father name)."""
    norm = normalize_arabic(query.strip())
    pattern = f"%{norm}%"
    raw_pattern = f"%{query.strip()}%"

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT p.*,
                   COUNT(DISTINCT pr.id) AS property_count,
                   COUNT(DISTINCT d.id) AS document_count,
                   GROUP_CONCAT(DISTINCT d.search_scope) AS search_scopes
            FROM persons p
            LEFT JOIN properties pr ON pr.person_id = p.id
            LEFT JOIN documents d ON d.person_id = p.id
            WHERE p.first_name_norm LIKE ?
               OR p.family_name_norm LIKE ?
               OR p.father_name LIKE ?
               OR p.first_name LIKE ?
               OR p.family_name LIKE ?
            GROUP BY p.id
            ORDER BY
                CASE WHEN p.first_name_norm = ? THEN 0
                     WHEN p.family_name_norm = ? THEN 0
                     ELSE 1 END,
                p.first_name
            """,
            (pattern, pattern, raw_pattern, raw_pattern, raw_pattern, norm, norm),
        ).fetchall()
    return [dict(r) for r in rows]


def search_properties(
    property_number: str = "",
    district: str = "",
    block: str = "",
) -> list[dict]:
    """Search properties by number, district, or block."""
    conditions = []
    params = []

    if property_number:
        conditions.append("pr.property_number LIKE ?")
        params.append(f"{property_number}%")

    if district:
        conditions.append("pr.real_estate_district LIKE ?")
        params.append(f"%{district}%")

    if block:
        conditions.append("pr.block LIKE ?")
        params.append(f"%{block}%")

    if not conditions:
        return []

    where = " AND ".join(conditions)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT pr.*,
                   p.first_name, p.father_name, p.family_name, p.family_origin,
                   d.search_scope
            FROM properties pr
            LEFT JOIN persons p ON p.id = pr.person_id
            LEFT JOIN documents d ON d.id = pr.document_id
            WHERE {where}
            ORDER BY pr.real_estate_district, pr.property_number
            """,
            params,
        ).fetchall()
    
    results = []
    for r in rows:
        row_dict = dict(r)
        if not row_dict.get("qaza") and row_dict.get("search_scope"):
            row_dict["qaza"] = row_dict["search_scope"]
        results.append(row_dict)
    return results


def get_person_with_properties(person_id: int) -> dict | None:
    """Fetch a person and all their properties."""
    with get_db() as conn:
        person = conn.execute(
            "SELECT * FROM persons WHERE id = ?", (person_id,)
        ).fetchone()
        if not person:
            return None

        props = conn.execute(
            """
            SELECT pr.*, d.image_path, d.id AS document_id, d.search_scope
            FROM properties pr
            LEFT JOIN documents d ON d.id = pr.document_id
            WHERE pr.person_id = ?
            ORDER BY pr.real_estate_district, pr.row_order
            """,
            (person_id,),
        ).fetchall()

        docs = conn.execute(
            "SELECT id, image_path, request_number, request_date, status, page_info, search_scope FROM documents WHERE person_id = ?",
            (person_id,),
        ).fetchall()

    properties_list = []
    doc_ids_with_props = set()
    for p in props:
        p_dict = dict(p)
        if not p_dict.get("qaza") and p_dict.get("search_scope"):
            p_dict["qaza"] = p_dict["search_scope"]
        properties_list.append(p_dict)
        doc_ids_with_props.add(p_dict.get("document_id"))

    # Add a "no properties" dummy row for documents that yielded no properties
    # so the search request is still recorded in the properties table
    docs_list = [dict(d) for d in docs]
    for d in docs_list:
        if d["id"] not in doc_ids_with_props:
            properties_list.append({
                "document_id": d["id"],
                "party_name": "-",
                "property_number": "-",
                "section": "-",
                "block": "-",
                "real_estate_district": "-",
                "qaza": d.get("search_scope") or "غير محدد",
                "num_shares": "-",
                "ownership_type": "لا يملك"
            })

    return {
        "person": dict(person),
        "properties": properties_list,
        "documents": docs_list,
    }

from fastapi import APIRouter, Request
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from services.search_service import (
    get_person_with_properties,
    search_persons,
    search_properties,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/search")
async def search(
    request: Request,
    q: str = "",
    property_number: str = "",
    district: str = "",
    block: str = "",
):
    persons = []
    properties = []
    selected_person = None

    if q:
        persons = search_persons(q)

    if property_number or district or block:
        properties = search_properties(property_number, district, block)

    # If exactly one person found, preload their full details
    if len(persons) == 1 and not properties:
        selected_person = get_person_with_properties(
            persons[0]["id"],
            persons[0].get("search_scope"),
        )

    return templates.TemplateResponse(
        request,
        "search.html",
        {
            "q": q,
            "property_number": property_number,
            "district": district,
            "block": block,
            "persons": persons,
            "properties": properties,
            "selected_person": selected_person,
        },
    )


@router.get("/persons/{person_id}")
async def person_detail(request: Request, person_id: int, search_scope: str = ""):
    data = get_person_with_properties(person_id, search_scope.strip() or None)
    if not data:
        return templates.TemplateResponse(
            request,
            "search.html",
            {"error": "Person not found", "q": "", "persons": [], "properties": [], "selected_person": None, "property_number": "", "district": "", "block": ""},
            status_code=404,
        )
    return templates.TemplateResponse(
        request,
        "person_detail.html",
        data,
    )


@router.get("/persons/{person_id}/export")
async def person_export_csv(person_id: int, qaza: str = ""):
    import io
    import csv
    
    data = get_person_with_properties(person_id, qaza.strip() or None)
    if not data:
        return Response("Person not found", status_code=404)
        
    person = data["person"]
    properties = data["properties"]
    
    output = io.StringIO()
    # Write BOM for Excel to open Arabic UTF-8 correctly
    output.write('\ufeff')
    writer = csv.writer(output)
    
    fullname_parts = [person.get("first_name", ""), person.get("father_name", ""), person.get("family_name", "")]
    fullname = " ".join(f for f in fullname_parts if f)
    
    writer.writerow(["بيانات الشخص"])
    writer.writerow(["الاسم كامل", fullname])
    writer.writerow(["رقم السجل", person.get("registry_number")])
    writer.writerow(["مكان السجل", person.get("registry_place")])
    writer.writerow(["تاريخ الولادة", person.get("birth_date")])
    writer.writerow(["الهوا / المنشأ", person.get("family_origin")])
    writer.writerow([])
    
    # Document-level info (search scope, request numbers)
    docs = data.get("documents", [])
    if docs:
        writer.writerow(["بيانات الوثائق"])
        for d in docs:
            parts = []
            if d.get("request_number"):
                parts.append(f"رقم الطلب: {d['request_number']}")
            if d.get("request_date"):
                parts.append(f"تاريخ: {d['request_date']}")
            if d.get("search_scope"):
                parts.append(f"القضاء: {d['search_scope']}")
            if d.get("page_info"):
                parts.append(f"صفحة: {d['page_info']}")
            if parts:
                writer.writerow(parts)
        writer.writerow([])
    
    writer.writerow(["العقارات المملوكة"])
    writer.writerow(["اسم الفريق", "رقم العقار", "القسم", "البلوك", "المنطقة العقارية", "القضاء", "عدد الأسهم", "نوع الملكية"])
    
    for p in properties:
        writer.writerow([
            p.get("party_name") or "",
            p.get("property_number") or "",
            p.get("section") or "",
            p.get("block") or "",
            p.get("real_estate_district") or "",
            p.get("qaza") or "",
            p.get("num_shares") or "",
            p.get("ownership_type") or ""
        ])
        
    content = output.getvalue()
    
    # Safe filename – use RFC 5987 encoding for Arabic characters
    from urllib.parse import quote
    safename = fullname.replace(" ", "_") or "person"
    if qaza:
        safe_qaza = qaza.replace(" ", "_").replace(":", "").replace("-", "")
        safename += f"_{safe_qaza}"
    
    encoded_name = quote(f"report_{safename}.csv")
        
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=report.csv; filename*=UTF-8''{encoded_name}"}
    )


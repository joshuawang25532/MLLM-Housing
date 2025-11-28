"""
Parser for Zillow property detail page data.
Converts raw scraped data into clean, structured JSON.
"""
import json
import re


def parse_scores_from_html(visible_content):
    """Parse Walk Score, Transit Score, and Bike Score from visible HTML content."""
    scores = {
        "walkScore": None,
        "transitScore": None,
        "bikeScore": None
    }
    
    rating_sections = visible_content.get("rating_sections", [])
    for rating in rating_sections:
        text = rating.get("text", "")
        
        # Parse Walk Score (format: "Walk Score®95 / 100")
        if "Walk Score" in text or "walk score" in text.lower():
            walk_match = re.search(r'Walk\s+Score[®\s]*(\d+)\s*/\s*100', text, re.IGNORECASE)
            if walk_match:
                scores["walkScore"] = int(walk_match.group(1))
        
        # Parse Transit Score
        if "Transit Score" in text or "transit score" in text.lower():
            transit_match = re.search(r'Transit\s+Score[®\s]*(\d+)\s*/\s*100', text, re.IGNORECASE)
            if transit_match:
                scores["transitScore"] = int(transit_match.group(1))
        
        # Parse Bike Score
        if "Bike Score" in text or "bike score" in text.lower():
            bike_match = re.search(r'Bike\s+Score[®\s]*(\d+)\s*/\s*100', text, re.IGNORECASE)
            if bike_match:
                scores["bikeScore"] = int(bike_match.group(1))
    
    return scores


def extract_clean_property_data(property_raw):
    """Extract and structure property data into clean, labeled JSON."""
    clean_data = {
        "basic_info": {},
        "location": {},
        "property_details": {},
        "financial": {},
        "schools": [],
        "scores": {},
        "history": {},
        "nearby": {},
        "features": {},
        "photos": {}
    }
    
    # Basic Info
    clean_data["basic_info"] = {
        "zpid": property_raw.get("zpid"),
        "address": property_raw.get("streetAddress"),
        "city": property_raw.get("city"),
        "state": property_raw.get("state"),
        "zipcode": property_raw.get("zipcode"),
        "bedrooms": property_raw.get("bedrooms"),
        "bathrooms": property_raw.get("bathrooms"),
        "livingArea": property_raw.get("livingArea"),
        "livingAreaUnits": property_raw.get("livingAreaUnits"),
        "lotSize": property_raw.get("lotSize"),
        "lotAreaValue": property_raw.get("lotAreaValue"),
        "lotAreaUnits": property_raw.get("lotAreaUnits"),
        "yearBuilt": property_raw.get("yearBuilt"),
        "homeType": property_raw.get("homeType"),
        "description": property_raw.get("description")
    }
    
    # Location
    clean_data["location"] = {
        "latitude": property_raw.get("latitude"),
        "longitude": property_raw.get("longitude"),
        "neighborhood": property_raw.get("neighborhoodRegion", {}).get("name") if property_raw.get("neighborhoodRegion") else None,
        "county": property_raw.get("county"),
        "timeZone": property_raw.get("timeZone")
    }
    
    # Property Details
    reso_facts = property_raw.get("resoFacts", {})
    clean_data["property_details"] = {
        "propertyCondition": reso_facts.get("propertyCondition"),
        "architecturalStyle": reso_facts.get("architecturalStyle"),
        "levels": reso_facts.get("levels"),
        "stories": reso_facts.get("storiesTotal"),
        "hasGarage": reso_facts.get("hasGarage"),
        "garageParkingCapacity": reso_facts.get("garageParkingCapacity"),
        "hasFireplace": reso_facts.get("hasFireplace"),
        "fireplaces": reso_facts.get("fireplaces"),
        "hasSpa": reso_facts.get("hasSpa"),
        "hasView": reso_facts.get("hasView"),
        "hasCooling": reso_facts.get("hasCooling"),
        "hasHeating": reso_facts.get("hasHeating"),
        "cooling": reso_facts.get("cooling"),
        "heating": reso_facts.get("heating"),
        "appliances": reso_facts.get("appliances"),
        "interiorFeatures": reso_facts.get("interiorFeatures"),
        "exteriorFeatures": reso_facts.get("exteriorFeatures"),
        "parkingFeatures": reso_facts.get("parkingFeatures"),
        "securityFeatures": reso_facts.get("securityFeatures"),
        "flooring": reso_facts.get("flooring"),
        "rooms": reso_facts.get("rooms")
    }
    
    # Financial
    clean_data["financial"] = {
        "price": property_raw.get("price"),
        "lastSoldPrice": property_raw.get("lastSoldPrice"),
        "dateSold": property_raw.get("dateSold"),
        "dateSoldString": property_raw.get("dateSoldString"),
        "zestimate": property_raw.get("zestimate"),
        "zestimateHighPercent": property_raw.get("zestimateHighPercent"),
        "zestimateLowPercent": property_raw.get("zestimateLowPercent"),
        "rentZestimate": property_raw.get("rentZestimate"),
        "taxAssessedValue": property_raw.get("taxAssessedValue"),
        "taxAssessedYear": property_raw.get("taxAssessedYear"),
        "propertyTaxRate": property_raw.get("propertyTaxRate"),
        "hoaFee": property_raw.get("hoaFee"),
        "monthlyHoaFee": property_raw.get("monthlyHoaFee"),
        "pricePerSquareFoot": reso_facts.get("pricePerSquareFoot") if reso_facts else None
    }
    
    # Schools
    schools_raw = property_raw.get("schools", [])
    clean_data["schools"] = [
        {
            "name": school.get("name"),
            "rating": school.get("rating"),
            "level": school.get("level"),
            "distance": school.get("distance"),
            "grades": school.get("grades"),
            "assigned": school.get("assigned"),
            "studentsPerTeacher": school.get("studentsPerTeacher"),
            "type": school.get("type"),
            "size": school.get("size")
        }
        for school in schools_raw
    ]
    
    # History
    clean_data["history"] = {
        "priceHistory": property_raw.get("priceHistory", []),
        "taxHistory": property_raw.get("taxHistory", [])
    }
    
    # Nearby
    clean_data["nearby"] = {
        "nearbyHomes": property_raw.get("nearbyHomes", []),
        "nearbyNeighborhoods": property_raw.get("nearbyNeighborhoods", []),
        "nearbyZipcodes": property_raw.get("nearbyZipcodes", []),
        "nearbyCities": property_raw.get("nearbyCities", [])
    }
    
    # Features (from resoFacts)
    if reso_facts:
        clean_data["features"] = {
            "hasAssociation": reso_facts.get("hasAssociation"),
            "associationFee": reso_facts.get("associationFee"),
            "associationFeeIncludes": reso_facts.get("associationFeeIncludes"),
            "hasPetsAllowed": reso_facts.get("hasPetsAllowed"),
            "hasHomeWarranty": reso_facts.get("hasHomeWarranty"),
            "hasLandLease": reso_facts.get("hasLandLease"),
            "isNewConstruction": reso_facts.get("isNewConstruction"),
            "numberOfUnitsInCommunity": reso_facts.get("numberOfUnitsInCommunity")
        }
    
    # Photos
    photo_count = property_raw.get("photoCount", 0)
    responsive_photos = property_raw.get("responsivePhotos", [])
    original_photos = property_raw.get("originalPhotos", [])
    thumb = property_raw.get("thumb", [])
    
    # Extract responsive photos (with multiple resolutions)
    photos_list = []
    for photo in responsive_photos:
        photo_data = {
            "url": photo.get("url"),
            "caption": photo.get("caption"),
            "subjectType": photo.get("subjectType"),
            "resolutions": {}
        }
        
        # Extract different resolutions from mixedSources
        mixed_sources = photo.get("mixedSources", {})
        if mixed_sources:
            # JPEG resolutions
            if "jpeg" in mixed_sources:
                photo_data["resolutions"]["jpeg"] = [
                    {
                        "url": img.get("url"),
                        "width": img.get("width")
                    }
                    for img in mixed_sources["jpeg"]
                ]
            # WebP resolutions
            if "webp" in mixed_sources:
                photo_data["resolutions"]["webp"] = [
                    {
                        "url": img.get("url"),
                        "width": img.get("width")
                    }
                    for img in mixed_sources["webp"]
                ]
        
        photos_list.append(photo_data)
    
    # Extract original photos (highest quality)
    original_photos_list = []
    for photo in original_photos:
        original_data = {
            "caption": photo.get("caption"),
            "resolutions": {}
        }
        
        mixed_sources = photo.get("mixedSources", {})
        if mixed_sources:
            if "jpeg" in mixed_sources:
                original_data["resolutions"]["jpeg"] = [
                    {
                        "url": img.get("url"),
                        "width": img.get("width")
                    }
                    for img in mixed_sources["jpeg"]
                ]
            if "webp" in mixed_sources:
                original_data["resolutions"]["webp"] = [
                    {
                        "url": img.get("url"),
                        "width": img.get("width")
                    }
                    for img in mixed_sources["webp"]
                ]
        
        original_photos_list.append(original_data)
    
    # Extract thumbnail
    thumbnail_url = None
    if thumb and isinstance(thumb, list) and len(thumb) > 0:
        thumbnail_url = thumb[0].get("url")
    elif isinstance(thumb, dict):
        thumbnail_url = thumb.get("url")
    
    clean_data["photos"] = {
        "photoCount": photo_count,
        "responsivePhotos": photos_list,
        "originalPhotos": original_photos_list,
        "thumbnail": thumbnail_url
    }
    
    return clean_data


def parse_detail_page_data(raw_data):
    """
    Parse raw scraped detail page data into clean JSON.
    
    Args:
        raw_data: dict containing:
            - property_raw: raw property data from gdpClientCache
            - scores_html: list of HTML text containing scores (optional)
            - url: original URL (optional)
            - scraped_url: actual scraped URL (optional)
    
    Returns:
        dict with clean, labeled property information
    """
    property_raw = raw_data.get("property_raw", {})
    scores_html = raw_data.get("scores_html", [])
    url = raw_data.get("url")
    scraped_url = raw_data.get("scraped_url")
    
    # Parse scores from HTML text
    visible_content = {"rating_sections": [{"text": text} for text in scores_html]}
    scores = parse_scores_from_html(visible_content)
    
    # Extract clean property data
    clean_data = extract_clean_property_data(property_raw)
    clean_data["scores"] = scores
    
    # Add metadata
    clean_data["metadata"] = {
        "url": url,
        "scrapedUrl": scraped_url,
        "zpid": property_raw.get("zpid")
    }
    
    return clean_data


def parse_from_next_data(next_data, scores_html=None, url=None, scraped_url=None):
    """
    Parse property data directly from __NEXT_DATA__ structure.
    
    Args:
        next_data: The __NEXT_DATA__ dict from the page
        scores_html: Optional list of HTML text containing scores
        url: Optional original URL
        scraped_url: Optional scraped URL
    
    Returns:
        dict with clean, labeled property information
    """
    # Extract property data from gdpClientCache
    comp_props = next_data.get("props", {}).get("pageProps", {}).get("componentProps", {})
    gdp_cache_str = comp_props.get("gdpClientCache", "")
    
    if not gdp_cache_str:
        return {"error": "Could not find property data in gdpClientCache"}
    
    gdp_cache = json.loads(gdp_cache_str)
    query_key = list(gdp_cache.keys())[0]
    property_raw = gdp_cache[query_key].get("property", {})
    
    if not property_raw:
        return {"error": "Could not extract property data"}
    
    raw_data = {
        "property_raw": property_raw,
        "scores_html": scores_html or [],
        "url": url,
        "scraped_url": scraped_url
    }
    
    return parse_detail_page_data(raw_data)


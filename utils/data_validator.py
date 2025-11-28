"""
Validator script for data/raw_houses directory.
Validates all house JSON files and reports issues.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


class HouseValidator:
    """Validates house JSON files."""
    
    def __init__(self, houses_dir: str = "data/raw_houses"):
        self.houses_dir = Path(houses_dir)
        self.errors = defaultdict(list)
        self.warnings = defaultdict(list)
        self.error_counts = defaultdict(int)  # Count occurrences of each error type
        self.warning_counts = defaultdict(int)  # Count occurrences of each warning type
        self.valid_count = 0
        self.invalid_count = 0
        self.issues_with_urls = []  # List of dicts with zpid, url, errors, warnings
        
    def _extract_url(self, filepath: Path, data: dict) -> Optional[str]:
        """Extract URL from house data, or construct from ZPID if not available."""
        # Try metadata.url first
        if "metadata" in data and "url" in data["metadata"]:
            return data["metadata"]["url"]
        
        # Try metadata.scrapedUrl
        if "metadata" in data and "scrapedUrl" in data["metadata"]:
            return data["metadata"]["scrapedUrl"]
        
        # Construct from ZPID if available
        zpid = None
        if "basic_info" in data and "zpid" in data["basic_info"]:
            zpid = data["basic_info"]["zpid"]
        elif "metadata" in data and "zpid" in data["metadata"]:
            zpid = data["metadata"]["zpid"]
        
        if zpid:
            return f"https://www.zillow.com/homedetails/{zpid}_zpid/"
        
        return None
    
    def validate_file(self, filepath: Path) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a single house file.
        
        Returns:
            (is_valid, errors, warnings)
        """
        errors = []
        warnings = []
        
        # Check 1: File exists and is readable
        if not filepath.exists():
            errors.append("File does not exist")
            return False, errors, warnings
        
        # Check 2: File is valid JSON
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON: {e}")
            return False, errors, warnings
        except Exception as e:
            errors.append(f"Error reading file: {e}")
            return False, errors, warnings
        
        # Check 3: File is a dictionary
        if not isinstance(data, dict):
            errors.append(f"Root element is not a dictionary (got {type(data).__name__})")
            return False, errors, warnings
        
        # Check 4: No error field indicating scraping failure
        if "error" in data:
            errors.append(f"Contains error field: {data.get('error')}")
            return False, errors, warnings
        
        # Check 5: Filename matches ZPID (if ZPID exists)
        expected_zpid = None
        if "basic_info" in data and data["basic_info"].get("zpid"):
            expected_zpid = str(data["basic_info"]["zpid"])
        elif "metadata" in data and data["metadata"].get("zpid"):
            expected_zpid = str(data["metadata"]["zpid"])
        
        if expected_zpid:
            filename_without_ext = filepath.stem
            if filename_without_ext != expected_zpid:
                warnings.append(f"Filename ({filename_without_ext}) does not match ZPID ({expected_zpid})")
        
        # Check 6: Required top-level sections exist
        required_sections = ["basic_info", "location", "financial"]
        for section in required_sections:
            if section not in data:
                errors.append(f"Missing required section: {section}")
        
        # Check 7: basic_info validation
        if "basic_info" in data:
            basic_info = data["basic_info"]
            
            # Required fields in basic_info
            required_fields = ["zpid", "address", "city", "state", "zipcode"]
            for field in required_fields:
                if field not in basic_info or basic_info[field] is None:
                    errors.append(f"basic_info missing required field: {field}")
            
            # Type checks
            if "zpid" in basic_info and basic_info["zpid"] is not None:
                if not isinstance(basic_info["zpid"], int):
                    errors.append(f"basic_info.zpid must be int (got {type(basic_info['zpid']).__name__})")
            
            if "bedrooms" in basic_info and basic_info["bedrooms"] is not None:
                if not isinstance(basic_info["bedrooms"], (int, float)):
                    warnings.append(f"basic_info.bedrooms should be numeric (got {type(basic_info['bedrooms']).__name__})")
            
            if "bathrooms" in basic_info and basic_info["bathrooms"] is not None:
                if not isinstance(basic_info["bathrooms"], (int, float)):
                    warnings.append(f"basic_info.bathrooms should be numeric (got {type(basic_info['bathrooms']).__name__})")
            
            if "livingArea" in basic_info and basic_info["livingArea"] is not None:
                if not isinstance(basic_info["livingArea"], (int, float)):
                    warnings.append(f"basic_info.livingArea should be numeric (got {type(basic_info['livingArea']).__name__})")
                elif basic_info["livingArea"] <= 0:
                    warnings.append(f"basic_info.livingArea should be positive (got {basic_info['livingArea']})")
        
        # Check 8: location validation
        if "location" in data:
            location = data["location"]
            
            # Latitude validation (critical - required!)
            if "latitude" not in location:
                errors.append("location.latitude is missing")
            elif location["latitude"] is None:
                errors.append("location.latitude is null (critical location feature)")
            elif not isinstance(location["latitude"], (int, float)):
                errors.append(f"location.latitude must be numeric (got {type(location['latitude']).__name__})")
            elif not (-90 <= location["latitude"] <= 90):
                errors.append(f"location.latitude out of range: {location['latitude']}")
            
            # Longitude validation (critical - required!)
            if "longitude" not in location:
                errors.append("location.longitude is missing")
            elif location["longitude"] is None:
                errors.append("location.longitude is null (critical location feature)")
            elif not isinstance(location["longitude"], (int, float)):
                errors.append(f"location.longitude must be numeric (got {type(location['longitude']).__name__})")
            elif not (-180 <= location["longitude"] <= 180):
                errors.append(f"location.longitude out of range: {location['longitude']}")
        
        # Check 9: financial validation
        if "financial" in data:
            financial = data["financial"]
            
            # Check lastSoldPrice (target variable - critical!)
            if "lastSoldPrice" not in financial:
                errors.append("financial.lastSoldPrice is missing")
            elif financial["lastSoldPrice"] is None:
                errors.append("financial.lastSoldPrice is null (target variable cannot be null)")
            elif not isinstance(financial["lastSoldPrice"], (int, float)):
                errors.append(f"financial.lastSoldPrice must be numeric (got {type(financial['lastSoldPrice']).__name__})")
            elif financial["lastSoldPrice"] <= 0:
                warnings.append(f"financial.lastSoldPrice should be positive (got {financial['lastSoldPrice']})")
            
            # Check price (optional, can be removed)
            if "price" in financial and financial["price"] is not None:
                if not isinstance(financial["price"], (int, float)):
                    warnings.append(f"financial.price should be numeric (got {type(financial['price']).__name__})")
                elif financial["price"] <= 0:
                    warnings.append(f"financial.price should be positive (got {financial['price']})")
        
        # Check 10: schools should be a list
        if "schools" in data:
            if not isinstance(data["schools"], list):
                warnings.append(f"schools should be a list (got {type(data['schools']).__name__})")
        
        # Check 11: scores validation
        # If basic_info exists (meaning __NEXT_DATA__ loaded), but all scores are None,
        # this indicates scores were not successfully scraped from HTML
        if "scores" in data and "basic_info" in data:
            scores = data["scores"]
            score_fields = ["walkScore", "transitScore", "bikeScore"]
            
            # Check if all scores are None - indicates scores weren't scraped from HTML
            # (since basic_info exists, __NEXT_DATA__ loaded, but HTML score extraction failed)
            all_scores_none = all(
                scores.get(field) is None 
                for field in score_fields 
                if field in scores
            )
            
            if all_scores_none:
                # If scores section exists but all values are None, this is an error
                # because it means HTML score extraction failed even though __NEXT_DATA__ loaded
                if len(scores) == 0 or all(v is None for v in scores.values()):
                    errors.append("All scores are None - __NEXT_DATA__ loaded but scores were not scraped from HTML (check failed_scores_canary.txt)")
            
            for field in score_fields:
                if field in scores and scores[field] is not None:
                    if not isinstance(scores[field], int):
                        warnings.append(f"scores.{field} should be int (got {type(scores[field]).__name__})")
                    elif not (0 <= scores[field] <= 100):
                        warnings.append(f"scores.{field} out of range [0-100]: {scores[field]}")
        elif "scores" not in data and "basic_info" in data:
            # Scores section missing entirely when basic_info exists - also indicates incomplete scraping
            errors.append("Missing scores section - __NEXT_DATA__ loaded but scores section is missing")
        
        # Check 12: photos validation
        if "photos" in data:
            photos = data["photos"]
            if "photoCount" in photos and photos["photoCount"] is not None:
                if not isinstance(photos["photoCount"], int):
                    warnings.append(f"photos.photoCount should be int (got {type(photos['photoCount']).__name__})")
                elif photos["photoCount"] < 0:
                    warnings.append(f"photos.photoCount should be non-negative (got {photos['photoCount']})")
        
        is_valid = len(errors) == 0
        return is_valid, errors, warnings
    
    def validate_all(self) -> Dict:
        """Validate all house files in the directory."""
        if not self.houses_dir.exists():
            print(f"❌ Directory '{self.houses_dir}' does not exist!")
            return {
                "valid_count": 0,
                "invalid_count": 0,
                "total_count": 0,
                "errors": {},
                "warnings": {}
            }
        
        json_files = list(self.houses_dir.glob("*.json"))
        total_count = len(json_files)
        
        print(f"Found {total_count} JSON files in '{self.houses_dir}'")
        print("=" * 70)
        
        for filepath in sorted(json_files):
            is_valid, errors, warnings = self.validate_file(filepath)
            
            if is_valid:
                self.valid_count += 1
            else:
                self.invalid_count += 1
            
            # Collect issues with URLs
            if errors or warnings:
                # Load data to extract URL
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    url = self._extract_url(filepath, data)
                    zpid = None
                    if "basic_info" in data and "zpid" in data["basic_info"]:
                        zpid = data["basic_info"]["zpid"]
                    elif "metadata" in data and "zpid" in data["metadata"]:
                        zpid = data["metadata"]["zpid"]
                    
                    self.issues_with_urls.append({
                        "zpid": zpid,
                        "filename": filepath.name,
                        "url": url,
                        "errors": errors,
                        "warnings": warnings
                    })
                except Exception:
                    # If we can't load the file, still record the issues
                    self.issues_with_urls.append({
                        "zpid": None,
                        "filename": filepath.name,
                        "url": None,
                        "errors": errors,
                        "warnings": warnings
                    })
            
            if errors:
                self.errors[filepath.name] = errors
                # Count each error type
                for error in errors:
                    self.error_counts[error] += 1
            
            if warnings:
                self.warnings[filepath.name] = warnings
                # Count each warning type
                for warning in warnings:
                    self.warning_counts[warning] += 1
        
        return {
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "total_count": total_count,
            "errors": dict(self.errors),
            "warnings": dict(self.warnings),
            "error_counts": dict(self.error_counts),
            "warning_counts": dict(self.warning_counts),
            "issues_with_urls": self.issues_with_urls
        }
    
    def save_issues_to_json(self, output_file: str, results: Optional[Dict] = None):
        """Save issues with URLs to a JSON file."""
        if results is None:
            results = {
                "issues_with_urls": self.issues_with_urls
            }
        
        issues_data = {
            "total_issues": len(results.get("issues_with_urls", [])),
            "houses_with_issues": results.get("issues_with_urls", [])
        }
        
        output_path = Path(output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(issues_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Saved {len(issues_data['houses_with_issues'])} houses with issues to: {output_path}")
    
    def print_report(self, results: Optional[Dict] = None):
        """Print validation report."""
        if results is None:
            results = {
                "valid_count": self.valid_count,
                "invalid_count": self.invalid_count,
                "total_count": self.valid_count + self.invalid_count,
                "errors": dict(self.errors),
                "warnings": dict(self.warnings)
            }
        
        print("\n" + "=" * 70)
        print("VALIDATION REPORT")
        print("=" * 70)
        print(f"\nTotal files: {results['total_count']}")
        print(f"✅ Valid: {results['valid_count']}")
        print(f"❌ Invalid: {results['invalid_count']}")
        
        # Display error/warning type statistics
        error_counts = results.get('error_counts', {})
        warning_counts = results.get('warning_counts', {})
        
        if error_counts:
            print(f"\n📊 ERROR TYPE STATISTICS:")
            print("-" * 70)
            # Sort by count (descending)
            for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {error_type}: {count} occurrence(s)")
        
        if warning_counts:
            print(f"\n📊 WARNING TYPE STATISTICS:")
            print("-" * 70)
            # Sort by count (descending)
            for warning_type, count in sorted(warning_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {warning_type}: {count} occurrence(s)")
            
            # Highlight the specific livingArea = 0 case
            living_area_warning = "basic_info.livingArea should be positive (got 0)"
            if living_area_warning in warning_counts:
                count = warning_counts[living_area_warning]
                print(f"\n⚠️  SPECIAL ATTENTION:")
                print(f"   '{living_area_warning}'")
                print(f"   Occurs in {count} file(s)")
        
        if not results['errors'] and not results['warnings']:
            print("\n✅ All files are valid!")
        
        print("\n" + "=" * 70)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate house JSON files in data/raw_houses directory")
    parser.add_argument(
        "--dir",
        type=str,
        default="data/raw_houses",
        help="Directory containing house JSON files (default: data/raw_houses)"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Validate a single file instead of all files"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    parser.add_argument(
        "--output-issues",
        type=str,
        help="Save houses with issues (URLs and errors/warnings) to a JSON file"
    )
    
    args = parser.parse_args()
    
    validator = HouseValidator(houses_dir=args.dir)
    
    if args.file:
        # Validate single file
        filepath = Path(args.file)
        if not filepath.is_absolute():
            # Try relative to current directory first, then relative to houses_dir
            if not filepath.exists():
                filepath = Path(args.dir) / filepath.name
        
        is_valid, errors, warnings = validator.validate_file(filepath)
        
        if args.json:
            result = {
                "file": str(filepath),
                "valid": is_valid,
                "errors": errors,
                "warnings": warnings
            }
            print(json.dumps(result, indent=2))
        else:
            print(f"Validating: {filepath.name}")
            print("=" * 70)
            if is_valid:
                print("✅ File is VALID")
            else:
                print("❌ File is INVALID")
            
            if errors:
                print("\nErrors:")
                for error in errors:
                    print(f"  • {error}")
            
            if warnings:
                print("\nWarnings:")
                for warning in warnings:
                    print(f"  • {warning}")
    else:
        # Validate all files
        results = validator.validate_all()
        
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            validator.print_report(results)
        
        # Save issues to JSON file if requested
        if args.output_issues:
            validator.save_issues_to_json(args.output_issues, results)


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Fix XML processing for TRTYRAG assignment data
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any

def extract_assignment_data(xml_file_path: str) -> List[Dict[str, Any]]:
    """Extract assignment data from TRTYRAG XML file"""
    
    print(f"Processing XML file: {xml_file_path}")
    
    records = []
    
    try:
        # Parse XML iteratively to handle large files
        for event, elem in ET.iterparse(xml_file_path, events=('end',)):
            if elem.tag == 'assignment-entry':
                record = extract_single_assignment(elem)
                if record:
                    records.append(record)
                elem.clear()  # Clear element to save memory
                
                # Print progress every 1000 records
                if len(records) % 1000 == 0:
                    print(f"Processed {len(records)} assignment records...")
    
    except Exception as e:
        print(f"Error processing XML: {e}")
    
    print(f"Total records extracted: {len(records)}")
    return records

def extract_single_assignment(assignment_elem: ET.Element) -> Dict[str, Any]:
    """Extract data from a single assignment-entry element"""
    
    record = {}
    
    try:
        # Extract assignment data
        assignment = assignment_elem.find('assignment')
        if assignment is not None:
            record['reel_no'] = get_text(assignment.find('reel-no'))
            record['frame_no'] = get_text(assignment.find('frame-no'))
            record['date_recorded'] = get_text(assignment.find('date-recorded'))
            record['conveyance_text'] = get_text(assignment.find('conveyance-text'))
            record['last_update_date'] = get_text(assignment.find('last-update-date'))
            record['purge_indicator'] = get_text(assignment.find('purge-indicator'))
            record['page_count'] = get_text(assignment.find('page-count'))
            
            # Extract correspondent data
            correspondent = assignment.find('correspondent')
            if correspondent is not None:
                record['correspondent_name'] = get_text(correspondent.find('person-or-organization-name'))
                record['correspondent_address1'] = get_text(correspondent.find('address-1'))
                record['correspondent_address2'] = get_text(correspondent.find('address-2'))
                record['correspondent_address3'] = get_text(correspondent.find('address-3'))
                record['correspondent_address4'] = get_text(correspondent.find('address-4'))
        
        # Extract assignor data (take first assignor)
        assignors = assignment_elem.find('assignors')
        if assignors is not None:
            assignor = assignors.find('assignor')
            if assignor is not None:
                record['assignor_name'] = get_text(assignor.find('person-or-organization-name'))
                record['assignor_city'] = get_text(assignor.find('city'))
                record['assignor_state'] = get_text(assignor.find('state'))
                record['assignor_country'] = get_text(assignor.find('country-name'))
                record['assignor_postcode'] = get_text(assignor.find('postcode'))
                record['assignor_execution_date'] = get_text(assignor.find('execution-date'))
                record['assignor_date_acknowledged'] = get_text(assignor.find('date-acknowledged'))
                record['assignor_legal_entity'] = get_text(assignor.find('legal-entity-text'))
                record['assignor_nationality'] = get_text(assignor.find('nationality'))
                record['assignor_address1'] = get_text(assignor.find('address-1'))
                record['assignor_address2'] = get_text(assignor.find('address-2'))
        
        # Extract assignee data (take first assignee)
        assignees = assignment_elem.find('assignees')
        if assignees is not None:
            assignee = assignees.find('assignee')
            if assignee is not None:
                record['assignee_name'] = get_text(assignee.find('person-or-organization-name'))
                record['assignee_city'] = get_text(assignee.find('city'))
                record['assignee_state'] = get_text(assignee.find('state'))
                record['assignee_country'] = get_text(assignee.find('country-name'))
                record['assignee_postcode'] = get_text(assignee.find('postcode'))
                record['assignee_legal_entity'] = get_text(assignee.find('legal-entity-text'))
                record['assignee_nationality'] = get_text(assignee.find('nationality'))
                record['assignee_address1'] = get_text(assignee.find('address-1'))
                record['assignee_address2'] = get_text(assignee.find('address-2'))
        
        # Extract property data (take first property)
        properties = assignment_elem.find('properties')
        if properties is not None:
            property_elem = properties.find('property')
            if property_elem is not None:
                record['serial_no'] = get_text(property_elem.find('serial-no'))
                record['registration_number'] = get_text(property_elem.find('registration-no'))
                record['intl_reg_no'] = get_text(property_elem.find('intl-reg-no'))
                
                # Extract trademark law treaty property
                tlt_property = property_elem.find('trademark-law-treaty-property')
                if tlt_property is not None:
                    record['tlt_mark_name'] = get_text(tlt_property.find('tlt-mark-name'))
                    record['tlt_mark_description'] = get_text(tlt_property.find('tlt-mark-description'))
        
        # Create assignment_id from reel_no and frame_no
        if record.get('reel_no') and record.get('frame_no'):
            record['assignment_id'] = f"{record['reel_no']}-{record['frame_no']}"
        
        return record
        
    except Exception as e:
        print(f"Error extracting assignment data: {e}")
        return None

def get_text(element: ET.Element) -> str:
    """Get text content from XML element, return empty string if None"""
    if element is not None and element.text:
        return element.text.strip()
    return ""

def test_xml_extraction():
    """Test the XML extraction with a small sample"""
    
    xml_file = "uspto_data/extracted/TRTYRAG/asb19550103-20241231-01/asb19550103-20241231-01.xml"
    
    print("Testing XML extraction for TRTYRAG...")
    print("=" * 50)
    
    # Extract first few records
    records = []
    count = 0
    
    try:
        for event, elem in ET.iterparse(xml_file, events=('end',)):
            if elem.tag == 'assignment-entry':
                record = extract_single_assignment(elem)
                if record:
                    records.append(record)
                    count += 1
                    
                    # Show first 3 records
                    if count <= 3:
                        print(f"\nRecord {count}:")
                        for key, value in record.items():
                            if value:  # Only show non-empty values
                                print(f"  {key}: {value}")
                
                elem.clear()
                
                # Stop after 10 records for testing
                if count >= 10:
                    break
                    
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\nExtracted {len(records)} records for testing")
    
    # Show summary of fields
    if records:
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        print(f"\nFields found: {sorted(all_fields)}")
        
        # Count non-null fields
        field_counts = {}
        for record in records:
            for field, value in record.items():
                if value:
                    field_counts[field] = field_counts.get(field, 0) + 1
        
        print(f"\nField usage (out of {len(records)} records):")
        for field, count in sorted(field_counts.items()):
            print(f"  {field}: {count}/{len(records)} ({count/len(records)*100:.1f}%)")

if __name__ == "__main__":
    test_xml_extraction()

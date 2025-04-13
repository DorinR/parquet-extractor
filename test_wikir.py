import ir_datasets
import os
import sys

def test_wikir_access():
    """
    Test script to diagnose issues with the ir-datasets wikir collection
    """
    print("Testing ir-datasets wikir access...")
    
    # 1. Check for available datasets
    print("\n1. Available wikir datasets:")
    try:
        # Use the correct method to get available dataset IDs
        for name in ir_datasets.registry:
            if 'wikir' in name:
                print(f"  - {name}")
    except Exception as e:
        print(f"  ✗ Error listing datasets: {e}")
    
    # 2. Try to load a specific wikir dataset
    dataset_name = 'wikir/en1k/validation'
    print(f"\n2. Attempting to load {dataset_name}...")
    try:
        dataset = ir_datasets.load(dataset_name)
        print(f"  ✓ Successfully loaded {dataset_name}")
    except Exception as e:
        print(f"  ✗ Failed to load {dataset_name}: {e}")
        return
    
    # 3. Check dataset metadata
    print("\n3. Dataset metadata:")
    try:
        print(f"  Dataset: {dataset}")
        print(f"  Documentation: {dataset.documentation()}")
    except Exception as e:
        print(f"  ✗ Error getting metadata: {e}")
    
    # 4. Try to access documents
    print("\n4. Attempting to access documents...")
    try:
        docs_iter = dataset.docs_iter()
        print(f"  ✓ Successfully accessed docs_iter()")
    except Exception as e:
        print(f"  ✗ Failed to access docs_iter(): {e}")
        return
    
    # 5. Check document structure
    print("\n5. Checking document structure...")
    try:
        doc = next(docs_iter)
        print(f"  Document type: {type(doc)}")
        print(f"  Document attributes: {[attr for attr in dir(doc) if not attr.startswith('_')]}")
        
        # Check for common attributes
        for attr in ['doc_id', 'text', 'title', 'body']:
            if hasattr(doc, attr):
                value = getattr(doc, attr)
                preview = str(value)[:50] + "..." if len(str(value)) > 50 else value
                print(f"  - {attr}: {preview}")
            else:
                print(f"  - {attr}: Not available")
    except Exception as e:
        print(f"  ✗ Error accessing document: {e}")
    
    # 6. Check multiple documents
    print("\n6. Checking first 5 documents...")
    try:
        docs_iter = dataset.docs_iter()
        for i, doc in enumerate(docs_iter):
            if i >= 5:
                break
            print(f"  Doc {i} (id: {doc.doc_id if hasattr(doc, 'doc_id') else 'unknown'})")
            # Check if it has at least some content
            has_content = False
            for attr in ['text', 'body']:
                if hasattr(doc, attr) and getattr(doc, attr):
                    has_content = True
                    break
            print(f"    Has content: {'Yes' if has_content else 'No'}")
    except Exception as e:
        print(f"  ✗ Error iterating documents: {e}")
    
    print("\nTest completed.")

if __name__ == "__main__":
    test_wikir_access() 
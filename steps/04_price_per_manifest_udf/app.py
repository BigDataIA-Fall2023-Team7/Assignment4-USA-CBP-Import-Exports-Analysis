import sys

def main(estimated_shipment_value: float, manifest_quantity: float) -> float:
    if estimated_shipment_value is None or manifest_quantity is None:
        return None
    
    return float(estimated_shipment_value)/float(manifest_quantity)


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore

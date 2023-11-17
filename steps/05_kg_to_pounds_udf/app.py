import sys
from quantities import units



def main(weight):
    if weight is None:
        return None

    weight_kg = float(weight) * units.kilogram
    weight_pounds = weight_kg.rescale(units.pound)
    return weight_pounds.magnitude


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore

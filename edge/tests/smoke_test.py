import sys
import logging

logging.basicConfig(level=logging.INFO)

def run_smoke_test():
    try:
        from openddil.telemetry.v1 import telemetry_pb2 as pb
        from detection.units import from_proto, to_proto
        import pint
        
        logging.info("1. Successfully imported telemetry_pb2 and detection.units")
        
        # Construct a proto Quantity
        q_proto = pb.Quantity(value=100.0, unit="[degF]")
        logging.info(f"2. Constructed Proto Quantity: {q_proto.value} {q_proto.unit}")
        
        # Convert to pint
        q_pint = from_proto(q_proto)
        logging.info(f"3. Converted to Pint Quantity: {q_pint}")
        
        # Convert to Kelvin
        q_kelvin = q_pint.to("kelvin")
        logging.info(f"4. Converted to Kelvin: {q_kelvin}")
        
        if round(q_kelvin.magnitude, 2) != 310.93:
            logging.error(f"Failed Kelvin conversion math. Expected ~310.93, got {q_kelvin.magnitude}")
            sys.exit(1)
            
        logging.info("SMOKE TEST PASSED.")
        sys.exit(0)
    except Exception as e:
        logging.error(f"SMOKE TEST FAILED: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_smoke_test()

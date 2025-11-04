from dotenv import load_dotenv
load_dotenv()

from app.dhan_client import DhanClient

if __name__ == "__main__":
    client = DhanClient()
    symbol = input("Enter symbol to inspect (e.g. RELIANCE): ").strip()

    instruments = client.instrument_search(symbol)
    print(f"\nFound {len(instruments)} instrument(s) for '{symbol}':\n")

    for i, inst in enumerate(instruments, 1):
        # filter here
        ex_type = str(inst.get("SEM_EXCH_INSTRUMENT_TYPE", "")).upper()
        instr_name = str(inst.get("SEM_INSTRUMENT_NAME", "")).upper()
        if ex_type == "EQ" or "EQUITY" in instr_name:
            print(f"#{i}:")
            print("SM_SYMBOL_NAME:", inst.get("SM_SYMBOL_NAME"))
            print("SEM_EXCH_INSTRUMENT_TYPE:", ex_type)
            print("SEM_INSTRUMENT_NAME:", instr_name)
            print("SEM_SMST_SECURITY_ID:", inst.get("SEM_SMST_SECURITY_ID"))
            print("-" * 40)

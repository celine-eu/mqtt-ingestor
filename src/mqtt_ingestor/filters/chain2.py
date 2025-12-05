def filter(doc):
    # drop documents whose payload contains keys starting with Chain2Response and Chain2Info
    if not isinstance(doc.payload, dict):
        return True

    for k in doc.payload:
        if k.startswith("Chain2Response") or k.startswith("Chain2Info"):
            return False

    return True

import sys
import time


def open_clf():
    try:
        import nfc
    except Exception:
        return None
    backends = [
        "usb",
        "tty:USB0",
        "tty:ACM0",
        "tty:AMA0",
        "tty:S0",
    ]
    for dev in backends:
        try:
            clf = nfc.ContactlessFrontend(dev)
            return clf
        except Exception:
            continue
    return None


def main():
    try:
        import nfc
    except ImportError:
        print("Falta dependencia: nfcpy. Instala con: pip install nfcpy pyusb")
        sys.exit(1)

    clf = open_clf()
    if not clf:
        print("No se encontró lector NFC compatible (usb/tty). Conecta un lector por USB/OTG y vuelve a intentar.")
        sys.exit(2)

    print("Lector NFC listo. Acerca una tarjeta...")
    with clf as c:
        while True:
            try:
                def on_connect(tag):
                    uid = getattr(tag, "identifier", b"")
                    if isinstance(uid, (bytes, bytearray)):
                        print("UID:", uid.hex().upper())
                    ndef_attr = getattr(tag, "ndef", None)
                    if ndef_attr:
                        try:
                            for record in ndef_attr.records:
                                print(record)
                        except Exception as e:
                            print("Error NDEF:", e)
                    return False

                c.connect(rdwr={"on-connect": on_connect})
            except KeyboardInterrupt:
                print("Saliendo...")
                break
            except Exception as e:
                print("Error:", e)
                time.sleep(1)


if __name__ == "__main__":
    main()


# -*- coding: utf8 -*-
###############################################################################
#                                aes_enc_dec.py                               #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2020-06-04
# Change history:

import sys
import seclib


def unhex(b_string):
    import codecs
    return codecs.encode(codecs.decode(b_string, 'hex'), 'base64').decode()


METHOD = ["ECB", 0, 1]
CODEPAGE = "UTF-16LE"
KEY = ""
# KEY = ""

# Description
# Wert eingeben und ver- bzw. entschlüsseln
if METHOD[1] == 1:
    _decrypted = "45266559"
    print("Verschlüsselung mit Methode {0}".format(METHOD[0]))
    print("Original: \"{0}\"".format(_decrypted))
    if METHOD[0] == "ECB":
        _encrypted = seclib.encrypt_ecb(KEY, _decrypted, CODEPAGE)
    elif METHOD[0] == "CBC":
        _encrypted = seclib.encrypt_cbc(KEY, _decrypted, CODEPAGE)
    else:
        print("Method {0} not supported.".format(METHOD))
        sys.exit(1)
    print("Verschlüsselter Wert: {0}".format(_encrypted))

if METHOD[2] == 1:
    _encrypted = "J9vDrAfR91R2GjYRMCCRmg=="
    print("\nEntschlüsselung mit Methode {0}".format(METHOD[0]))
    print("Verschlüsselter Wert: {0}".format(_encrypted))
    if METHOD[0] == "ECB":
        _decrypted = seclib.decrypt_ecb(KEY, _encrypted, CODEPAGE)
    elif METHOD[0] == "CBC":
        _decrypted = seclib.decrypt_cbc(KEY, _encrypted, CODEPAGE)
    else:
        print("Method {0} not supported.".format(METHOD))
        sys.exit(1)
    print("Entschlüsseltes Original: \"{0}\"".format(_decrypted))

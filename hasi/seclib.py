# -*- coding: utf8 -*-
###############################################################################
#                                  seclib.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains helper functions for security
import base64
import logging
import os

standard_encoding = "utf8"
cred_file = ".connection_keystore"

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")

# Workaround for Windows OS
if not os.environ.get('HOME'):
    os.environ['HOME'] = os.path.expanduser('~')


def get_credentials(key):
    """ Parses a hidden credentials file from user's home directory and return list of credentials. """
    try:
        with open("{}/{}".format(os.environ['HOME'], cred_file), 'r') as pass_file:
            for line in pass_file:
                # when key matches, return values for credentials
                sec_pattern = line.split('|')
                if sec_pattern[0] == key:
                    # 4 values will be interpreted as key, target host, user and password
                    if len(sec_pattern) == 4:
                        log.debug("Credentials saved for {} on {}...".format(sec_pattern[2], sec_pattern[1]))
                        return [sec_pattern[1], sec_pattern[2], sec_pattern[3].strip()]
                    # 5 values will be interpreted as key, target host, service, user and password
                    elif len(sec_pattern) == 5:
                        log.debug("Credentials saved for {} on {}...".format(sec_pattern[3], sec_pattern[1]))
                        return [sec_pattern[1], sec_pattern[2], sec_pattern[3], sec_pattern[4].strip()]
            """ Key not in file """
            log.error("Key {0} cannot be found in Keystore.".format(key))
            return False
    except FileNotFoundError as e:
        log.error(e)
        return False
    except PermissionError as e:
        log.error(e)
        return False


def encrypt_ecb(key, s_data, codepage, manual_padding=False, truncate_key=None):
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    b_key = base64.b64decode(key)
    i = 16
    if manual_padding is True:
        pad_size = (i - len(s_data) % i + len(s_data)) if len(s_data) % i >= 0 else len(s_data)
        s_data = s_data.ljust(pad_size, '\0')
    byte_object = s_data.encode(codepage)
    byte_object = pad(byte_object, 16, style='pkcs7') if len(byte_object) % i != 0 else byte_object
    cipher = AES.new(b_key, AES.MODE_ECB)
    _encrypted = cipher.encrypt(byte_object)
    if truncate_key:
        return base64.b64encode(_encrypted).decode(standard_encoding)[:truncate_key]
    else:
        return base64.b64encode(_encrypted).decode(standard_encoding)


def decrypt_ecb(key, s_data, codepage):
    from Crypto.Cipher import AES
    b_key = base64.b64decode(key.encode(codepage))
    byte_object = base64.b64decode(s_data.encode(codepage))
    cipher = AES.new(b_key, AES.MODE_ECB)
    _decrypted = cipher.decrypt(byte_object)
    return _decrypted.decode(codepage).rstrip('\0')


def encrypt_cbc(key, s_data, codepage, iv=b'0000000000000000'):
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    b_key = base64.b64decode(key)
    cipher = AES.new(b_key, AES.MODE_CBC, iv)
    bytes_object = pad(s_data.encode(codepage), 16, style='pkcs7')
    _encrypted = cipher.encrypt(bytes_object)
    return base64.b64encode(_encrypted).decode(standard_encoding)


def decrypt_cbc(key, s_data, codepage, iv=b'0000000000000000'):
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    b_key = base64.b64decode(key.encode(codepage))
    byte_object = base64.b64decode(s_data.encode(codepage))
    cipher = AES.new(b_key, AES.MODE_CBC, iv)
    _decrypted = unpad(cipher.decrypt(byte_object), 16, style='pkcs7')
    return _decrypted.decode(codepage).rstrip('\0')

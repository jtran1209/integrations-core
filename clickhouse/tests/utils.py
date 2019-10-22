# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from hashlib import sha256


def hash_password(password):
    hexed = sha256(password.encode('utf-8')).hexdigest()

    # Odd length hex can cause issues
    if len(hexed) & 1:
        hexed = '0' + hexed

    return hexed

[app]
title = SUPA Thermo NFC
package.name = supa_thermo_nfc
package.domain = org.supa
source.dir = .
source.include_exts = py,kv
version = 0.1
requirements = python3, kivy, pyjnius
orientation = portrait
fullscreen = 0
android.permissions = NFC
android.features = android.hardware.nfc:required=true
android.api = 34
android.minapi = 24
android.archs = arm64-v8a

[buildozer]
log_level = 2
warn_on_root = 1

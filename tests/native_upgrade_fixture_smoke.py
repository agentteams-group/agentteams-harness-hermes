"""History proof must not accidentally resemble an outbound credential."""
from agent.redact import redact_for_egress

# Exact synthetic markers from failing CI44179805992. The digit-only suffix
# plus colon/nonce matches Telegram token syntax; it is not missing history.
seed = "578f657434ed4086991319b026325816"
nonce = "11f483cf53af4135868fe28c8e12f379"
assert redact_for_egress("RESUMED:" + seed + ":" + nonce) != "RESUMED:" + seed + ":" + nonce
expected = "RESUMED:" + seed + "|" + nonce
assert redact_for_egress(expected) == expected
print("Native egress preserves the exact history and nonce proof without credential-shaped separators")

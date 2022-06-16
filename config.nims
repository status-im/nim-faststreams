# begin Nimble config (version 1)
when fileExists("nimble.paths"):
  include "nimble.paths"
# end Nimble config

if defined(windows) and not defined(vcc):
  # Avoid some rare stack corruption while using exceptions with a SEH-enabled
  # toolchain: https://github.com/status-im/nimbus-eth2/issues/3121
  switch("define", "nimRawSetjmp")

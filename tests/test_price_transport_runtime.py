"""Current cash transport remains portable outside the original Codex runtime."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import price_transport

class NodeRuntimeTests(unittest.TestCase):
 def test_explicit_missing_runtime_fails_without_launching_a_provider(self):
  with patch.dict(os.environ,{'CS2_CASH_NODE':str(Path(tempfile.gettempdir())/'cs2-missing-node-executable')},clear=False),patch.object(price_transport.subprocess,'Popen')as spawn:
   result=price_transport.fetch('skinport_live')
  self.assertIsNone(result['status']);self.assertIn('Node runtime unavailable',result['error']);spawn.assert_not_called()

 def test_path_runtime_works_without_private_configuration(self):
  existing=shutil.which('node')or shutil.which('nodejs')
  if not existing:self.skipTest('Node is not installed on this test host')
  with patch.dict(os.environ,{},clear=False):
   os.environ.pop('CS2_CASH_NODE',None)
   binary=price_transport.node_binary()
   completed=subprocess.run([str(binary),'--version'],capture_output=True,text=True,timeout=5,check=True)
  self.assertEqual(binary,Path(existing).resolve());self.assertRegex(completed.stdout.strip(),r'^v\d+\.\d+\.\d+$')

 def test_explicit_executable_takes_precedence_over_path(self):
  with tempfile.TemporaryDirectory()as folder:
   target=Path(folder)/'operator-selected-node';target.touch()
   with patch.dict(os.environ,{'CS2_CASH_NODE':str(target)},clear=False),patch.object(price_transport.shutil,'which')as search:
    self.assertEqual(price_transport.node_binary(),target)
   search.assert_not_called()

if __name__=='__main__':unittest.main()

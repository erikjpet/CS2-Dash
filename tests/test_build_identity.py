import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import build_identity

class BuildIdentityTests(unittest.TestCase):
 def test_fingerprint_tracks_runtime_but_ignores_private_state(self):
  with tempfile.TemporaryDirectory()as folder,patch.dict(os.environ,{'CS2DASH_SOURCE_REVISION':''},clear=False):
   root=Path(folder);(root/'server.py').write_text('pass\n');(root/'index.html').write_text('<main>Test</main>')
   before=build_identity.capture(root,'1.0.1');(root/'.env').write_text('private');(root/'data').mkdir();(root/'data/cs2dash.sqlite').write_bytes(b'private');(root/'report.html').write_text('private generated report')
   self.assertEqual(before,build_identity.capture(root,'1.0.1'))
   (root/'index.html').write_text('<main>Changed</main>')
   self.assertNotEqual(before['runtimeSha256'],build_identity.capture(root,'1.0.1')['runtimeSha256'])
   self.assertEqual(before['runtimeFileCount'],2);self.assertIsNone(before['sourceRevision'])

 def test_explicit_published_revision_is_validated(self):
  with tempfile.TemporaryDirectory()as folder,patch.dict(os.environ,{'CS2DASH_SOURCE_REVISION':'a'*40},clear=False):
   record=build_identity.capture(folder,'1.0.1');self.assertEqual(record['sourceRevision'],'a'*40);self.assertEqual(record['revisionOrigin'],'environment')
  with tempfile.TemporaryDirectory()as folder,patch.dict(os.environ,{'CS2DASH_SOURCE_REVISION':'not-a-revision'},clear=False):
   self.assertIsNone(build_identity.capture(folder,'1.0.1')['sourceRevision'])

if __name__=='__main__':unittest.main()

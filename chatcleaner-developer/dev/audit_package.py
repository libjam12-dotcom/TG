"""Offline distributable-content audit. Reports positions and categories, never secrets."""
from pathlib import Path
import ast,re,json,sys
ROOT=Path(__file__).resolve().parents[1]
FORBIDDEN={'.git','.env','.client-access.txt','tdata','node_modules','__pycache__','.local-data','.venv'}
patterns={
 'private_key':r'-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----',
 'bot_token':r'\b\d{6,12}:[A-Za-z0-9_-]{25,}\b',
 'provider_token':r'\b(?:sk-|AIza|ghp_|github_pat_)[A-Za-z0-9_-]{16,}',
 'production_domain':r'chatcleaner-\d+-\d+-\d+-\d+\.sslip\.io',
 'personal_filesystem':r'/' + r'Users/[^\s\x22\x27]+',
 'credential_url':r'https?://[^\s/@\x22\x27]+:[^\s/@\x22\x27]+@',
}
issues=[];fixture_credentials=[];count=0
for f in sorted(ROOT.rglob('*')):
 rel=str(f.relative_to(ROOT))
 if any(p in FORBIDDEN for p in f.relative_to(ROOT).parts):
  issues.append({'file':rel,'kind':'excluded_runtime_path'});continue
 if f.is_symlink():issues.append({'file':rel,'kind':'symlink'});continue
 if not f.is_file():continue
 count+=1
 if f.suffix in ('.zip','.sqlite','.sqlite3','.db','.session','.pem','.key','.log','.pyc'):
  issues.append({'file':rel,'kind':'forbidden_type'});continue
 try:s=f.read_text()
 except UnicodeError:issues.append({'file':rel,'kind':'unreviewed_binary'});continue
 if f.suffix=='.py':
  try:ast.parse(s)
  except SyntaxError:issues.append({'file':rel,'kind':'python_syntax'})
 for kind,pat in patterns.items():
  for m in re.finditer(pat,s):
   line=s.count('\n',0,m.start())+1
   # Deliberate invalid credential URLs in negative parser tests, never configured access.
   if kind=='credential_url' and '/tests/' in '/'+rel:fixture_credentials.append({'file':rel,'line':line,'kind':kind})
   else:issues.append({'file':rel,'line':line,'kind':kind})
print(json.dumps({'files_checked':count,'issues':issues,'synthetic_negative_test_credentials':fixture_credentials,'pass':not issues},ensure_ascii=False,indent=2))
sys.exit(bool(issues))

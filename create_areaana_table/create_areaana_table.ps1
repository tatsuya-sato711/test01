Param(
  [Parameter(Mandatory)]$targetMonth,
  [Parameter(Mandatory)]$schemaName
)
$script:scriptPath = $MyInvocation.MyCommand.Path
Function OutLogInfo { echo (@((Get-Date -Format 'yyy-MM-dd HH:mm:ss'), '[', (Split-Path -Leaf $scriptPath), ']', 'INFO:',  ($args -join ' ')) -join ' ') }
Function OutLogErr  { echo (@((Get-Date -Format 'yyy-MM-dd HH:mm:ss'), '[', (Split-Path -Leaf $scriptPath), ']', 'ERROR:', ($args -join ' ')) -join ' ') }

# 処理開始
OutLogInfo 'start'
cd (Split-Path -Parent $scriptPath)
OutLogInfo '$pwd:' $pwd.ToString()
OutLogInfo '$targetMonth:' $targetMonth

$psqlExe = 'C:\Program Files\PostgreSQL\9.6\bin\psql.exe'

# 一時ログファイル
$tmpLogFile = './tmp.log'

# SQL雛形
$createSqlTemplateFile = './create_areaana_table.sql'
OutLogInfo '$createSqlTemplateFile:' $createSqlTemplateFile

$insertSqlTemplateFile = './insert_areaana_table.sql'
OutLogInfo '$insertSqlTemplateFile:' $insertSqlTemplateFile

$vacuumSqlTemplateFile = './vacuum_areaana_table.sql'
OutLogInfo '$vacuumSqlTemplateFile:' $vacuumSqlTemplateFile

# 一時SQLファイル
$tmpSqlFile = './tmp.sql'

# スキーマ
OutLogInfo '$schemaName:' $schemaName

# 滞留テーブル作成実行
OutLogInfo 'create areaana table start'

# SQL文字列を作成しUTF-8（BOMなし）で保存
(Get-Content $createSqlTemplateFile -Encoding UTF8).Replace('[schema]', $schemaName).Replace('[yyyymm]', $targetMonth)`
| Out-String `
| % { [Text.Encoding]::UTF8.GetBytes($_) } `
| Set-Content $tmpSqlFile -Encoding Byte

Set-Item env:PGPASSWORD -Value **********
Set-Item env:PGCLIENTENCODING -Value UNICODE

& $psqlExe `
-f $tmpSqlFile `
-e `
-h secure-geocluster.cswoz8ktixcx.ap-northeast-1.redshift.amazonaws.com `
-p 5439 `
-U sys_dmp `
-d geodb > $tmpLogFile 2>&1

# 終了判定
#   $?だとINFOレベルのログがある場合もFalseになり、
#   $LASTEXITCODEだと逆にERRORがあっても0が返ってきてしまうので
#   ログに'ERROR:'が出力されているかどうかで判断する
$ret = -not (Get-Content $tmpLogFile | Out-String).contains('ERROR:')

# 処理終了
If (-not $ret) { OutLogErr 'create areaana table failed'; Exit 1 }
OutLogInfo 'create areaana table end'

OutLogInfo 'insert areaana table start'

# SQL文字列を作成しUTF-8（BOMなし）で保存
(Get-Content $insertSqlTemplateFile -Encoding UTF8).Replace('[schema]', $schemaName).Replace('[yyyymm]', $targetMonth) `
| Out-String `
| % { [Text.Encoding]::UTF8.GetBytes($_) } `
| Set-Content $tmpSqlFile -Encoding Byte

$RetryCount = 2
$RetryWaitseconds = 600

$snsArn = 'arn:aws:sns:ap-northeast-1:467774776521:geo-segmentation-data-dim'
$messegeRetry_insert = '[geo-segmentation-data-dim]ERROR: ./insert_areaana_table.sql failed. retry 10mins later.'

# VACUUM同時処理や何らかのエラーにより落ちた場合に再実行する。
$ErrorActionPreference = 'Stop'
while ($true) {
  try {
    & $psqlExe`
    -f $tmpSqlFile`
    -e`
    -h secure-geocluster.cswoz8ktixcx.ap-northeast-1.redshift.amazonaws.com`
    -p 5439`
    -U sys_dmp`
    -d geodb > $tmpLogFile 2>&1
    break
  } catch { 
    OutLogErr 'An error has occurred. Retry after 10mins.'
    aws sns publish --topic-arn $snsArn --message $messegeRetry_insert --subject $messageRetry
    if ($RetryCount -gt 0) {
      Start-Sleep -seconds $RetryWaitseconds
      $RetryCount--
    } else {
      OutLogErr 'reach the upper retrylimit. insert areaana table failed'
      Exit 1
    }
  }
}
OutLogInfo 'insert areaana table end'

OutLogInfo 'vacuum areaana table start'

# SQL文字列を作成しUTF-8（BOMなし）で保存
(Get-Content $vacuumSqlTemplateFile -Encoding UTF8).Replace('[schema]', $schemaName).Replace('[yyyymm]', $targetMonth) `
| Out-String `
| % { [Text.Encoding]::UTF8.GetBytes($_) } `
| Set-Content $tmpSqlFile -Encoding Byte

$RetryCount = 2
$RetryWaitseconds = 600

$messegeRetry_vacuum = '[geo-segmentation-data-dim]ERROR:  ./vacuum_areaana_table.sql failed. retry 10mins later.'

# VACUUM同時処理や何らかのエラーにより落ちた場合に再実行する。
$ErrorActionPreference = 'Stop'
while ($true) {
  try {
    & $psqlExe `
    -f $tmpSqlFile `
    -e `
    -h secure-geocluster.cswoz8ktixcx.ap-northeast-1.redshift.amazonaws.com `
    -p 5439 `
    -U sys_dmp `
    -d geodb > $tmpLogFile 2>&1
    break
 } catch { 
    OutLogErr 'An error has occurred. Retry after 10mins.'
    aws sns publish --topic-arn $snsArn --message $messegeRetry_vacuum --subject $messageRetry
    if ($RetryCount -gt 0) {
      Start-Sleep -seconds $RetryWaitseconds
      $RetryCount--
    } else {
      OutLogErr 'reach the upper retrylimit. vacuum areaana table failed'
      Exit 1
    }
  }
}

# 終了処理
OutLogInfo 'vacuum areaana table end'
Exit 0

testtesttest;
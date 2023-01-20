Param(
  [Parameter(Mandatory)]$targetMonth,
  [Parameter(Mandatory)]$schemaName
)
$script:scriptPath = $MyInvocation.MyCommand.Path
Function OutLogInfo { echo (@((Get-Date -Format 'yyy-MM-dd HH:mm:ss'), '[', (Split-Path -Leaf $scriptPath), ']', 'INFO:',  ($args -join ' ')) -join ' ') }
Function OutLogErr  { echo (@((Get-Date -Format 'yyy-MM-dd HH:mm:ss'), '[', (Split-Path -Leaf $scriptPath), ']', 'ERROR:', ($args -join ' ')) -join ' ') }


OutLogInfo 'start'
cd (Split-Path -Parent $scriptPath)
OutLogInfo '$pwd:' $pwd.ToString()
OutLogInfo '$targetMonth:' $targetMonth

$psqlExe = 'C:\Program Files\PostgreSQL\9.6\bin\psql.exe'


$tmpLogFile = './tmp.log'


$createSqlTemplateFile = './create_areaana_table.sql'
OutLogInfo '$createSqlTemplateFile:' $createSqlTemplateFile

$insertSqlTemplateFile = './insert_areaana_table.sql'
OutLogInfo '$insertSqlTemplateFile:' $insertSqlTemplateFile

$vacuumSqlTemplateFile = './vacuum_areaana_table.sql'
OutLogInfo '$vacuumSqlTemplateFile:' $vacuumSqlTemplateFile


$tmpSqlFile = './tmp.sql'


OutLogInfo '$schemaName:' $schemaName

OutLogInfo 'create areaana table start'

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


$ret = -not (Get-Content $tmpLogFile | Out-String).contains('ERROR:')


If (-not $ret) { OutLogErr 'create areaana table failed'; Exit 1 }
OutLogInfo 'create areaana table end'

OutLogInfo 'insert areaana table start'


(Get-Content $insertSqlTemplateFile -Encoding UTF8).Replace('[schema]', $schemaName).Replace('[yyyymm]', $targetMonth) `
| Out-String `
| % { [Text.Encoding]::UTF8.GetBytes($_) } `
| Set-Content $tmpSqlFile -Encoding Byte


$RetryCount = 2

$RetryWaitseconds = 600

$snsArn = 'arn:aws:sns:ap-northeast-1:467774776521:geo-segmentation-data-dim'
$messegeRetry = '[geo-segmentation-data-dim]ERROR: ./create_areaana_table/create_areaana_table.ps1 failed retry 10mins later.'

$ErrorActionPreference = "Stop"


while ($true){
  try{
    & $psqlExe`
    -f $tmpSqlFile`
    -e`
    -h secure-geocluster.cswoz8ktixcx.ap-northeast-1.redshift.amazonaws.com`
    -p 5439`
    -U sys_dmp`
    -d geodb > $tmpLogFile 2>&1
    }catch { 
        OutLogErr 'An error has occurred. Retry after 10mins.'
        aws sns publish --topic-arn $snsArn --message $messegeRetry --subject $messageRetry;
        if ($RetryCount -gt 0){
          Start-Sleep -seconds $RetryWaitseconds
          $RetryCount--
        }else{
          OutLogErr "reach the upper retrylimit. insert areaana table failed";
          Exit 1;
             }
           }
         }
  

OutLogInfo 'insert areaana table end'

OutLogInfo 'vacuum areaana table start'

(Get-Content $vacuumSqlTemplateFile -Encoding UTF8).Replace('[schema]', $schemaName).Replace('[yyyymm]', $targetMonth) `
| Out-String `
| % { [Text.Encoding]::UTF8.GetBytes($_) } `
| Set-Content $tmpSqlFile -Encoding Byte


$RetryCount = 2

$RetryWaitseconds = 600

$ErrorActionPreference = "Stop"
while ($true){
  try{ 
    & $psqlExe 
    -f $tmpSqlFile`
    -e`
    -h secure-geocluster.cswoz8ktixcx.ap-northeast-1.redshift.amazonaws.com`
    -p 5439`
    -U sys_dmp`
    -d geodb > $tmpLogFile 2>&1 
    }catch { 
        OutLogErr ?gAn error has occurred. Retry after 10mins.?h
        aws sns publish --topic-arn $snsArn --message $messegeRetry --subject $messageRetry;
        if ($RetryCount -gt 0){
          Start-Sleep -seconds $RetryWaitseconds
          $RetryCount--
        }else{
            OutLogErr "reach the upper retrylimit. vacuum areaana table failed";
            Exit 1;
             }
          }
      }
    

OutLogInfo 'vacuum areaana table end'
Exit 0
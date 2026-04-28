$lines = Get-Content 'C:\Users\Chase\.claude\projects\C--Users-Chase-Projects-GameFlowData\d1125247-f498-4f08-89a5-08c11dc8159b.jsonl'
$output = @()
foreach ($line in $lines) {
    try {
        $obj = $line | ConvertFrom-Json -ErrorAction Stop
        if ($obj.type -in ('user','assistant') -and -not $obj.isMeta) {
            $role = $obj.message.role.ToUpper()
            $content = $obj.message.content
            if ($content -is [string]) {
                $output += "[$role]: $content"
                $output += ""
            } elseif ($content -is [array]) {
                foreach ($block in $content) {
                    if ($block.type -eq 'text' -and $block.text) {
                        $output += "[$role]: $($block.text)"
                        $output += ""
                    }
                }
            }
        }
    } catch {}
}
$output | Out-File 'C:\Users\Chase\Desktop\recovered_session.txt' -Encoding utf8
Write-Host "Extracted $($output.Count) message blocks"

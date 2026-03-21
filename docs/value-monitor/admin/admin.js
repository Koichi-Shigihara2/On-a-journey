// 設定データの保持
let sectors = [];
let items = [];
let tickers = [];
let monitorTickers = [];

// 初期読み込み
async function loadAllConfigs() {
    try {
        // セクター (YAML)
        const sectorsResp = await fetch('../config/sectors.yaml');
        const sectorsText = await sectorsResp.text();
        sectors = jsyaml.load(sectorsText).sectors || [];
        
        // 調整科目 (JSON)
        const itemsResp = await fetch('../config/adjustment_items.json');
        items = await itemsResp.json();
        
        // 銘柄マスタ (CSV)
        const tickersResp = await fetch('../config/cik_lookup.csv');
        const tickersText = await tickersResp.text();
        tickers = parseCSV(tickersText);
        
        // 監視銘柄 (YAML)
        const monitorResp = await fetch('../config/monitor_tickers.yaml');
        const monitorText = await monitorResp.text();
        monitorTickers = jsyaml.load(monitorText).tickers || [];
        
        renderAll();
    } catch (error) {
        console.error('設定読み込みエラー:', error);
        alert('設定ファイルの読み込みに失敗しました。');
    }
}

// CSVパース
function parseCSV(text) {
    const lines = text.trim().split('\n');
    const headers = lines[0].split(',');
    return lines.slice(1).map(line => {
        const values = line.split(',');
        let obj = {};
        headers.forEach((h, i) => obj[h.trim()] = values[i]?.trim() || '');
        return obj;
    });
}

// CSV生成
function generateCSV(data) {
    if (data.length === 0) return '';
    const headers = Object.keys(data[0]);
    const lines = [headers.join(',')];
    data.forEach(row => {
        lines.push(headers.map(h => row[h] || '').join(','));
    });
    return lines.join('\n');
}

// レンダリング
function renderAll() {
    renderSectors();
    renderItems();
    renderTickers();
    document.getElementById('monitor-tickers').value = monitorTickers.join(', ');
}

// セクター表示
function renderSectors() {
    const container = document.getElementById('sectors-list');
    let html = '';
    sectors.forEach((sector, idx) => {
        html += `
        <div class="border rounded p-4 bg-gray-50">
            <div class="flex justify-between mb-2">
                <input type="text" value="${sector.name}" placeholder="セクター名" class="border rounded px-2 py-1 w-64" data-sector-idx="${idx}" data-field="name">
                <button onclick="removeSector(${idx})" class="text-red-600 hover:text-red-800">削除</button>
            </div>
            <div class="mb-2">
                <label class="block text-sm">キーワード（カンマ区切り）</label>
                <input type="text" value="${sector.keywords.join(', ')}" class="border rounded px-2 py-1 w-full" data-sector-idx="${idx}" data-field="keywords">
            </div>
            <div class="mb-2">
                <label class="block text-sm">SICコード（カンマ区切り）</label>
                <input type="text" value="${(sector.sic_codes || []).join(', ')}" class="border rounded px-2 py-1 w-full" data-sector-idx="${idx}" data-field="sic_codes">
            </div>
            <div>
                <label class="block text-sm">デフォルト除外項目（IDカンマ区切り）</label>
                <input type="text" value="${(sector.exclusions || []).map(e => e.item_id).join(', ')}" class="border rounded px-2 py-1 w-full" data-sector-idx="${idx}" data-field="exclusions">
            </div>
        </div>
        `;
    });
    container.innerHTML = html;
    
    // 入力変更イベント
    document.querySelectorAll('[data-sector-idx]').forEach(input => {
        input.addEventListener('change', function(e) {
            const idx = this.dataset.sectorIdx;
            const field = this.dataset.field;
            const value = this.value;
            if (field === 'keywords') {
                sectors[idx].keywords = value.split(',').map(s => s.trim());
            } else if (field === 'sic_codes') {
                sectors[idx].sic_codes = value.split(',').map(s => s.trim());
            } else if (field === 'exclusions') {
                sectors[idx].exclusions = value.split(',').map(id => ({ item_id: id.trim() }));
            } else {
                sectors[idx][field] = value;
            }
        });
    });
}

// 調整科目表示
function renderItems() {
    // 省略（同様のロジック）
}

// 銘柄表示
function renderTickers() {
    const tbody = document.getElementById('tickers-table');
    let html = '';
    tickers.forEach((ticker, idx) => {
        html += `
        <tr>
            <td class="py-2 px-4 border-b"><input type="text" value="${ticker.ticker}" class="w-full border rounded px-2 py-1" data-ticker-idx="${idx}" data-field="ticker"></td>
            <td class="py-2 px-4 border-b"><input type="text" value="${ticker.cik}" class="w-full border rounded px-2 py-1" data-ticker-idx="${idx}" data-field="cik"></td>
            <td class="py-2 px-4 border-b"><input type="text" value="${ticker.name || ''}" class="w-full border rounded px-2 py-1" data-ticker-idx="${idx}" data-field="name"></td>
            <td class="py-2 px-4 border-b">
                <select class="border rounded px-2 py-1" data-ticker-idx="${idx}" data-field="sector">
                    <option value="">未設定</option>
                    ${sectors.map(s => `<option value="${s.name}" ${ticker.sector === s.name ? 'selected' : ''}>${s.name}</option>`).join('')}
                </select>
            </td>
            <td class="py-2 px-4 border-b">
                <button onclick="removeTicker(${idx})" class="text-red-600 hover:text-red-800">削除</button>
            </td>
        </tr>
        `;
    });
    tbody.innerHTML = html;
    
    // 変更イベント
    document.querySelectorAll('[data-ticker-idx]').forEach(input => {
        input.addEventListener('change', function(e) {
            const idx = this.dataset.tickerIdx;
            const field = this.dataset.field;
            tickers[idx][field] = this.value;
        });
    });
}

// 保存処理
async function saveSectors() {
    const yamlStr = jsyaml.dump({ sectors });
    await saveToGitHub('config/sectors.yaml', yamlStr);
}

async function saveItems() {
    const jsonStr = JSON.stringify(items, null, 2);
    await saveToGitHub('config/adjustment_items.json', jsonStr);
}

async function saveTickers() {
    const csvStr = generateCSV(tickers);
    await saveToGitHub('config/cik_lookup.csv', csvStr);
}

async function saveMonitor() {
    const tickers = document.getElementById('monitor-tickers').value.split(',').map(s => s.trim());
    const yamlStr = jsyaml.dump({ tickers });
    await saveToGitHub('config/monitor_tickers.yaml', yamlStr);
}

// GitHub API連携（オプション）
async function saveToGitHub(path, content) {
    // 個人アクセストークンを要求
    const token = prompt('GitHub Personal Access Tokenを入力してください（publicリポジトリへの書き込み権限必要）:');
    if (!token) return;
    
    try {
        // 現在のファイルのSHAを取得（更新に必要）
        const repo = 'Koichi-Shigihara2/Adjusted-EPS-Analyzer'; // あなたのリポジトリ名
        const url = `https://api.github.com/repos/${repo}/contents/${path}`;
        const getResp = await axios.get(url, {
            headers: { Authorization: `token ${token}` }
        });
        const sha = getResp.data.sha;
        
        // ファイル更新
        const contentBase64 = btoa(unescape(encodeURIComponent(content)));
        await axios.put(url, {
            message: `Update ${path} via admin UI`,
            content: contentBase64,
            sha: sha,
            branch: 'main'
        }, {
            headers: { Authorization: `token ${token}` }
        });
        alert('保存成功！ GitHubに反映されました。');
    } catch (error) {
        console.error('GitHub API error:', error);
        alert('保存失敗: ' + error.message);
    }
}

// ダウンロード
function downloadConfig(filename) {
    let content = '';
    if (filename.endsWith('.yaml')) {
        if (filename === 'sectors.yaml') content = jsyaml.dump({ sectors });
        else if (filename === 'monitor_tickers.yaml') content = jsyaml.dump({ tickers: monitorTickers });
    } else if (filename.endsWith('.json')) {
        content = JSON.stringify(items, null, 2);
    } else if (filename.endsWith('.csv')) {
        content = generateCSV(tickers);
    }
    
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

// 初期化
window.onload = loadAllConfigs;

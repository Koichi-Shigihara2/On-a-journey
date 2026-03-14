async function loadData() {
    const ticker = 'PLTR'; // 表示したい銘柄名
    try {
        // '../' をつけることで docs フォルダから出て data フォルダを探しに行きます
        const response = await fetch(`../data/${ticker}/latest.json`);
        const data = await response.json();

        // 画面の要素にデータを流し込む
        document.getElementById('date').innerText = data.date;
        document.getElementById('eps').innerText = data.adjusted_eps.toFixed(4);
        // AIのコメントを表示
        document.getElementById('ai-comment').innerText = data.ai_comment || "解説準備中...";

    } catch (error) {
        console.error("データの読み込みに失敗しました:", error);
    }
}
loadData();

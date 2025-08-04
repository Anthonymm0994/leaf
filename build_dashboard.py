#!/usr/bin/env python3
"""
Build a fully offline dashboard from any Arrow file with specified columns.

Usage:
    python build_dashboard.py <arrow_file> <column1> <column2> ... [--output <output_file>]

Example:
    python build_dashboard.py data.arrow age income rating score --output my_dashboard.html
"""

import json
import sys
import argparse
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np
import base64
import requests

def download_library(url):
    """Download a library and return its content"""
    print(f"  Downloading: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def encode_typed_array(arr):
    """Encode numpy array to base64 for embedding"""
    if hasattr(arr, 'values'):  # Handle pandas Series
        arr = arr.values
    
    # Convert to appropriate dtype
    if arr.dtype == np.float64:
        arr = arr.astype(np.float32)
    elif arr.dtype == np.int64:
        arr = arr.astype(np.uint32)
    elif arr.dtype == np.int32:
        arr = arr.astype(np.uint32)
    elif arr.dtype == np.int16:
        arr = arr.astype(np.uint16)
    elif arr.dtype == np.int8:
        arr = arr.astype(np.uint8)
    
    # Determine dtype string
    dtype_map = {
        np.float32: 'float32',
        np.uint32: 'uint32',
        np.uint16: 'uint16',
        np.uint8: 'uint8'
    }
    
    dtype = dtype_map.get(arr.dtype.type, 'float32')
    
    return {
        'dtype': dtype,
        'shape': list(arr.shape),
        'data': base64.b64encode(arr.tobytes()).decode('ascii')
    }

def process_arrow_file(arrow_file, columns):
    """Process Arrow file and extract specified columns"""
    print(f"\nProcessing Arrow file: {arrow_file}")
    
    # Read Arrow file
    with open(arrow_file, 'rb') as f:
        reader = pa.ipc.open_stream(f)
        table = reader.read_all()
    
    print(f"Total rows: {table.num_rows}")
    print(f"Available columns: {table.column_names}")
    
    # Validate columns
    for col in columns:
        if col not in table.column_names:
            print(f"Warning: Column '{col}' not found in Arrow file")
            return None
    
    # Extract data
    data = {}
    metadata = {
        'total_rows': table.num_rows,
        'columns': columns,
        'version': '1.0'
    }
    
    for col in columns:
        column = table[col]
        
        # Convert to numpy array based on type
        if pa.types.is_integer(column.type):
            arr = column.to_numpy()
        elif pa.types.is_floating(column.type):
            arr = column.to_numpy()
        elif pa.types.is_boolean(column.type):
            arr = column.to_numpy().astype(np.uint8)
        elif pa.types.is_timestamp(column.type) or pa.types.is_date(column.type):
            # Convert to float (seconds since epoch)
            arr = column.to_numpy().astype(np.float64) / 1e9
            arr = arr.astype(np.float32)
        else:
            # For string/other types, try to convert to categorical
            try:
                unique_values = pc.unique(column).to_pylist()
                value_map = {v: i for i, v in enumerate(unique_values)}
                arr = np.array([value_map.get(v, 0) for v in column.to_pylist()], dtype=np.uint16)
                print(f"  Column '{col}' converted to categorical with {len(unique_values)} unique values")
            except:
                print(f"  Warning: Could not process column '{col}' of type {column.type}")
                continue
        
        data[col] = encode_typed_array(arr)
        print(f"  Processed column '{col}': {arr.shape[0]} values")
    
    return {'metadata': metadata, 'data': data}

def create_dashboard_html(data_json, columns):
    """Create the dashboard HTML with embedded data and libraries"""
    
    print("\nDownloading required libraries...")
    libs = {
        'vega': download_library('https://cdn.jsdelivr.net/npm/vega@5/build/vega.min.js'),
        'vega-lite': download_library('https://cdn.jsdelivr.net/npm/vega-lite@5/build/vega-lite.min.js'),
        'vega-embed': download_library('https://cdn.jsdelivr.net/npm/vega-embed@6/build/vega-embed.min.js')
    }
    
    html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Data Dashboard</title>
    <style>
        * { box-sizing: border-box; }
        
        body {
            background: #0d1117;
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 0;
        }
        
        .header {
            background: #161b22;
            border-bottom: 1px solid #30363d;
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .header h1 {
            color: #58a6ff;
            font-size: 20px;
            margin: 0;
        }
        
        .controls-bar {
            background: #161b22;
            border-bottom: 1px solid #30363d;
            padding: 10px 20px;
            display: flex;
            gap: 20px;
            align-items: center;
            flex-wrap: wrap;
        }
        
        .control-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .control-group label {
            color: #8b949e;
            font-size: 13px;
        }
        
        select, input[type="range"] {
            background: #0d1117;
            color: #c9d1d9;
            border: 1px solid #30363d;
            padding: 5px 10px;
            border-radius: 6px;
            font-size: 13px;
        }
        
        input[type="range"] {
            width: 150px;
        }
        
        .main-container {
            display: flex;
            gap: 20px;
            padding: 20px;
        }
        
        #visualization {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
            flex: 1;
        }
        
        .stats-panel {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
            width: 300px;
        }
        
        .stats-panel h3 {
            color: #58a6ff;
            margin-top: 0;
            margin-bottom: 15px;
        }
        
        .stat-group {
            margin-bottom: 20px;
        }
        
        .stat-group h4 {
            color: #c9d1d9;
            margin: 10px 0 5px 0;
            font-size: 14px;
        }
        
        .stat-item {
            display: flex;
            justify-content: space-between;
            padding: 3px 0;
            font-size: 13px;
        }
        
        .stat-label {
            color: #8b949e;
        }
        
        .stat-value {
            color: #c9d1d9;
            font-weight: 600;
        }
        
        button {
            background: #238636;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 13px;
            font-weight: 500;
        }
        
        button:hover {
            background: #2ea043;
        }
        
        button.secondary {
            background: #21262d;
        }
        
        button.secondary:hover {
            background: #30363d;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #8b949e;
        }
        
        .performance-info {
            color: #7ee787;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div style="display: flex; align-items: center;">
            <h1>Data Dashboard</h1>
        </div>
        <div>
            <button onclick="exportSelected()">Export Selected</button>
            <button onclick="exportAll()" class="secondary">Export All</button>
            <button onclick="clearSelection()" class="secondary">Clear Selection</button>
        </div>
    </div>
    
    <div class="controls-bar">
        <div class="control-group">
            <label>X Axis:</label>
            <select id="xAxis" onchange="updateVisualization()"></select>
        </div>
        <div class="control-group">
            <label>Y Axis:</label>
            <select id="yAxis" onchange="updateVisualization()"></select>
        </div>
        <div class="control-group">
            <label>Scatter Points:</label>
            <input type="range" id="scatterSize" min="1000" max="20000" value="5000" step="1000" 
                   oninput="updateScatterSize(this.value)">
            <span id="scatterSizeLabel">5,000</span>
        </div>
        <div class="control-group performance-info">
            <span>Render: <span id="renderTime">-</span>ms</span>
        </div>
    </div>
    
    <div class="main-container">
        <div id="visualization">
            <div class="loading">Loading visualization...</div>
        </div>
        
        <div class="stats-panel">
            <h3>Summary Statistics</h3>
            <div id="stats-content">
                <div class="stat-group">
                    <h4>Selection</h4>
                    <div class="stat-item">
                        <span class="stat-label">Total Rows:</span>
                        <span class="stat-value" id="totalRows">-</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Selected:</span>
                        <span class="stat-value" id="selectedRows">-</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Percentage:</span>
                        <span class="stat-value" id="selectedPercent">-</span>
                    </div>
                </div>
                <div id="column-stats"></div>
            </div>
        </div>
    </div>
    
    <script>
"""
    
    # Add libraries
    html += "\n        // Vega\n        " + libs['vega'] + "\n"
    html += "\n        // Vega-Lite\n        " + libs['vega-lite'] + "\n"
    html += "\n        // Vega-Embed\n        " + libs['vega-embed'] + "\n"
    
    html += """
    </script>
    
    <script>
        const embeddedData = """ + data_json + """;
        
        let dashboard = {
            data: [],
            columns: [],
            totalRows: 0,
            scatterSampleSize: 5000,
            vegaView: null,
            xAxis: null,
            yAxis: null,
            typedData: {},
            selection: null
        };
        
        function decodeTypedArray(encoded) {
            const { dtype, data } = encoded;
            const bytes = atob(data);
            const buffer = new ArrayBuffer(bytes.length);
            const view = new Uint8Array(buffer);
            
            for (let i = 0; i < bytes.length; i++) {
                view[i] = bytes.charCodeAt(i);
            }
            
            switch(dtype) {
                case 'float32': return new Float32Array(buffer);
                case 'uint32': return new Uint32Array(buffer);
                case 'uint16': return new Uint16Array(buffer);
                case 'uint8': return new Uint8Array(buffer);
                default: throw new Error('Unknown dtype: ' + dtype);
            }
        }
        
        async function initDashboard() {
            try {
                console.log('Initializing dashboard...');
                
                dashboard.columns = embeddedData.metadata.columns;
                dashboard.totalRows = embeddedData.metadata.total_rows;
                
                // Decode typed arrays
                for (const col of dashboard.columns) {
                    dashboard.typedData[col] = decodeTypedArray(embeddedData.data[col]);
                }
                
                // Convert to row format
                dashboard.data = [];
                for (let i = 0; i < dashboard.totalRows; i++) {
                    const row = {};
                    for (const col of dashboard.columns) {
                        row[col] = dashboard.typedData[col][i];
                    }
                    dashboard.data.push(row);
                }
                
                initializeControls();
                await createVisualization();
                updateStats();
                
            } catch (error) {
                console.error('Error:', error);
                document.getElementById('visualization').innerHTML = 
                    '<div style="color: #f85149; padding: 20px;">Error: ' + error.message + '</div>';
            }
        }
        
        function initializeControls() {
            const xSelect = document.getElementById('xAxis');
            const ySelect = document.getElementById('yAxis');
            
            dashboard.xAxis = dashboard.columns[0];
            dashboard.yAxis = dashboard.columns[1];
            
            dashboard.columns.forEach((col, i) => {
                xSelect.innerHTML += `<option value="${col}" ${col === dashboard.xAxis ? 'selected' : ''}>${col}</option>`;
                ySelect.innerHTML += `<option value="${col}" ${col === dashboard.yAxis ? 'selected' : ''}>${col}</option>`;
            });
            
            document.getElementById('totalRows').textContent = dashboard.totalRows.toLocaleString();
        }
        
        async function createVisualization() {
            const startTime = performance.now();
            
            const step = Math.max(1, Math.floor(dashboard.totalRows / dashboard.scatterSampleSize));
            const sampleData = [];
            for (let i = 0; i < dashboard.totalRows; i += step) {
                if (sampleData.length >= dashboard.scatterSampleSize) break;
                sampleData.push(dashboard.data[i]);
            }
            
            // Observable pattern
            const brush = {
                "name": "brush",
                "select": {
                    "type": "interval",
                    "encodings": ["x"],
                    "resolve": "intersect"
                }
            };
            
            const hist = {
                "mark": "bar",
                "encoding": {
                    "x": {
                        "field": {"repeat": "row"},
                        "type": "quantitative",
                        "bin": {"maxbins": 30}
                    },
                    "y": {
                        "aggregate": "count",
                        "type": "quantitative",
                        "title": null
                    }
                }
            };
            
            const histLayer = {
                "layer": [
                    {
                        ...hist,
                        "params": [brush],
                        "encoding": {
                            ...hist.encoding,
                            "color": {"value": "lightgrey"}
                        }
                    },
                    {
                        ...hist,
                        "transform": [{"filter": {"param": "brush"}}],
                        "encoding": {
                            ...hist.encoding,
                            "color": {"value": "#1f77b4"}
                        }
                    }
                ],
                "width": 400,
                "height": 100
            };
            
            const scatter = {
                "data": {"values": sampleData},
                "mark": {
                    "type": "point",
                    "tooltip": true
                },
                "width": 400,
                "height": 400,
                "encoding": {
                    "x": {
                        "field": dashboard.xAxis,
                        "type": "quantitative",
                        "scale": {"zero": false}
                    },
                    "y": {
                        "field": dashboard.yAxis,
                        "type": "quantitative",
                        "scale": {"zero": false}
                    },
                    "color": {
                        "condition": {
                            "param": "brush",
                            "value": "#1f77b4"
                        },
                        "value": "grey"
                    },
                    "opacity": {
                        "condition": {
                            "param": "brush",
                            "value": 0.8
                        },
                        "value": 0.1
                    }
                }
            };
            
            const spec = {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "data": {"values": dashboard.data},
                "hconcat": [
                    {
                        "repeat": {"row": dashboard.columns},
                        "spec": histLayer
                    },
                    scatter
                ],
                "config": {
                    "background": "#161b22",
                    "view": {"stroke": null},
                    "axis": {
                        "labelColor": "#8b949e",
                        "titleColor": "#c9d1d9",
                        "gridColor": "#21262d",
                        "domainColor": "#30363d"
                    }
                }
            };
            
            document.getElementById('visualization').innerHTML = '';
            const result = await vegaEmbed('#visualization', spec, {
                actions: false,
                theme: 'dark'
            });
            
            dashboard.vegaView = result.view;
            
            result.view.addSignalListener('brush', (name, value) => {
                dashboard.selection = value;
                updateStats(value);
            });
            
            const renderTime = performance.now() - startTime;
            document.getElementById('renderTime').textContent = renderTime.toFixed(1);
        }
        
        async function updateVisualization() {
            dashboard.xAxis = document.getElementById('xAxis').value;
            dashboard.yAxis = document.getElementById('yAxis').value;
            await createVisualization();
        }
        
        async function updateScatterSize(value) {
            dashboard.scatterSampleSize = parseInt(value);
            document.getElementById('scatterSizeLabel').textContent = parseInt(value).toLocaleString();
            await createVisualization();
        }
        
        function updateStats(selection) {
            let selectedIndices = [];
            
            if (!selection || Object.keys(selection).length === 0) {
                document.getElementById('selectedRows').textContent = 'All';
                document.getElementById('selectedPercent').textContent = '100%';
                selectedIndices = Array.from({length: dashboard.totalRows}, (_, i) => i);
            } else {
                // Find selected rows
                for (let i = 0; i < dashboard.totalRows; i++) {
                    let selected = true;
                    for (const [field, range] of Object.entries(selection)) {
                        if (dashboard.typedData[field][i] < range[0] || 
                            dashboard.typedData[field][i] > range[1]) {
                            selected = false;
                            break;
                        }
                    }
                    if (selected) selectedIndices.push(i);
                }
                
                document.getElementById('selectedRows').textContent = selectedIndices.length.toLocaleString();
                document.getElementById('selectedPercent').textContent = 
                    (selectedIndices.length / dashboard.totalRows * 100).toFixed(1) + '%';
            }
            
            // Calculate stats for each column
            const statsHtml = dashboard.columns.map(col => {
                const values = selectedIndices.map(i => dashboard.typedData[col][i]);
                const stats = calculateStats(values);
                
                return `
                    <div class="stat-group">
                        <h4>${col}</h4>
                        <div class="stat-item">
                            <span class="stat-label">Min:</span>
                            <span class="stat-value">${stats.min.toFixed(2)}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Max:</span>
                            <span class="stat-value">${stats.max.toFixed(2)}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Mean:</span>
                            <span class="stat-value">${stats.mean.toFixed(2)}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Median:</span>
                            <span class="stat-value">${stats.median.toFixed(2)}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Std Dev:</span>
                            <span class="stat-value">${stats.stdDev.toFixed(2)}</span>
                        </div>
                    </div>
                `;
            }).join('');
            
            document.getElementById('column-stats').innerHTML = statsHtml;
        }
        
        function calculateStats(values) {
            const sorted = values.slice().sort((a, b) => a - b);
            const n = values.length;
            const sum = values.reduce((a, b) => a + b, 0);
            const mean = sum / n;
            
            const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / n;
            const stdDev = Math.sqrt(variance);
            
            return {
                min: sorted[0],
                max: sorted[n - 1],
                mean: mean,
                median: n % 2 === 0 ? (sorted[n/2 - 1] + sorted[n/2]) / 2 : sorted[Math.floor(n/2)],
                stdDev: stdDev
            };
        }
        
        function exportSelected() {
            const selection = dashboard.vegaView ? 
                dashboard.vegaView.signal('brush') : {};
            
            const data = [];
            for (const row of dashboard.data) {
                if (!selection || Object.keys(selection).length === 0) {
                    data.push(row);
                } else {
                    let selected = true;
                    for (const [field, range] of Object.entries(selection)) {
                        if (row[field] < range[0] || row[field] > range[1]) {
                            selected = false;
                            break;
                        }
                    }
                    if (selected) data.push(row);
                }
            }
            
            exportData(data, 'selected');
        }
        
        function exportAll() {
            exportData(dashboard.data, 'all');
        }
        
        function exportData(data, suffix) {
            const csv = [
                dashboard.columns.join(','),
                ...data.map(row => dashboard.columns.map(col => row[col]).join(','))
            ].join('\\n');
            
            const blob = new Blob([csv], {type: 'text/csv'});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `data_${suffix}_${new Date().toISOString().slice(0,10)}.csv`;
            a.click();
            URL.revokeObjectURL(url);
        }
        
        function clearSelection() {
            if (dashboard.vegaView) {
                dashboard.vegaView.signal('brush', {}).run();
                updateStats({});
            }
        }
        
        initDashboard();
    </script>
</body>
</html>"""
    
    return html

def main():
    parser = argparse.ArgumentParser(description='Build offline dashboard from Arrow file')
    parser.add_argument('arrow_file', help='Path to Arrow file')
    parser.add_argument('columns', nargs='+', help='Columns to include in dashboard')
    parser.add_argument('--output', default='dashboard.html', help='Output HTML file (default: dashboard.html)')
    
    args = parser.parse_args()
    
    # Process Arrow file
    data = process_arrow_file(args.arrow_file, args.columns)
    if not data:
        print("Failed to process Arrow file")
        return 1
    
    # Create dashboard
    print("\nCreating dashboard...")
    data_json = json.dumps(data)
    html = create_dashboard_html(data_json, args.columns)
    
    # Save output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\n✅ Dashboard created successfully!")
    print(f"   Output: {output_path}")
    print(f"   Size: {len(html) / 1024 / 1024:.2f} MB")
    print(f"   Columns: {', '.join(args.columns)}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
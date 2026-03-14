import React, { useState, useRef } from 'react';
import Papa from 'papaparse';
import { Upload, FileType, CheckCircle2, Play, AlertCircle, BarChart2 } from 'lucide-react';

interface AlertProps {
  type: 'success' | 'error' | 'info';
  message: string;
}

const Alert = ({ type, message }: AlertProps) => {
  const colors = {
    success: 'bg-green-500/10 text-green-400 border-green-500/20',
    error: 'bg-red-500/10 text-red-400 border-red-500/20',
    info: 'bg-blue-500/10 text-blue-400 border-blue-500/20'
  };
  
  const Icon = type === 'success' ? CheckCircle2 : type === 'error' ? AlertCircle : Play;

  return (
    <div className={`flex items-center gap-3 p-4 rounded-lg border ${colors[type]} mb-6 animate-in fade-in slide-in-from-top-4`}>
      <Icon className="w-5 h-5 flex-shrink-0" />
      <p className="text-sm font-medium">{message}</p>
    </div>
  );
};

export default function App() {
  const [file, setFile] = useState<File | null>(null);
  const [goal, setGoal] = useState('');
  const [mode, setMode] = useState<'llm' | 'deterministic'>('llm');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<any>(null);
  const [previewData, setPreviewData] = useState<{ headers: string[], rows: any[][] } | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
      setError(null);
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile.name.endsWith('.csv')) {
        setFile(droppedFile);
        setError(null);
      } else {
        setError("Please upload a valid CSV file.");
      }
    }
  };

  const loadCsvPreview = async (url: string) => {
    try {
      // Fetch the CSV file using the correct server URL
      const response = await fetch(`http://localhost:8000${url}`);
      if (!response.ok) throw new Error("Failed to fetch resulting CSV");
      
      const csvText = await response.text();
      
      Papa.parse(csvText, {
        header: true,
        preview: 100, // Only show first 100 rows
        skipEmptyLines: true,
        complete: (results) => {
          if (results.data && results.data.length > 0) {
            const headers = Object.keys(results.data[0] as object);
            const rows = results.data.map((row: any) => headers.map(h => row[h]));
            setPreviewData({ headers, rows });
          }
        }
      });
    } catch (err) {
      console.error("Error loading CSV preview:", err);
      setError("Failed to load data preview.");
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file) {
      setError("Please select a file first.");
      return;
    }
    if (mode === 'llm' && !goal.trim()) {
      setError("Please enter a transformation goal.");
      return;
    }

    setIsLoading(true);
    setError(null);
    setResult(null);
    setPreviewData(null);

    const formData = new FormData();
    formData.append('file', file);
    if (mode === 'llm') formData.append('goal', goal);

    const endpoint = mode === 'llm' ? 'http://localhost:8000/process' : 'http://localhost:8000/transform';

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        body: formData,
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.detail || data.error || "An error occurred during processing.");
      }

      setResult(data);
      
      // Load preview data
      const downloadUrl = data.download_url || data.download_cleaned_url;
      if (downloadUrl) {
        await loadCsvPreview(downloadUrl);
      }

    } catch (err: any) {
      setError(err.message || "Failed to connect to the server. Is the FastAPI backend running?");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[var(--color-background)] text-[var(--color-text-main)] py-12 px-4 sm:px-6 lg:px-8 bg-[radial-gradient(ellipse_at_top,_var(--color-surface),_var(--color-background),_var(--color-background))]">
      <div className="max-w-5xl mx-auto space-y-8">
        
        {/* Header */}
        <header className="text-center space-y-4">
          <div className="inline-flex items-center justify-center p-3 glass-panel mb-4 shadow-blue-500/20">
            <BarChart2 className="w-8 h-8 text-[var(--color-primary)]" />
          </div>
          <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-400">
            DIANA
          </h1>
          <p className="text-lg text-[var(--color-text-muted)] max-w-2xl mx-auto">
            Autonomous ETL via Natural Language. Upload your raw data, tell the AI what you want, and instantly preview the clean results.
          </p>
        </header>

        <main className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          
          {/* Controls Column */}
          <div className="lg:col-span-5 space-y-6">
            <div className="glass-panel p-6 sm:p-8">
              
              {/* Mode Toggle */}
              <div className="flex bg-slate-800/50 p-1 rounded-xl mb-8 border border-white/5">
                <button
                  onClick={() => setMode('llm')}
                  className={`flex-1 py-2.5 px-4 rounded-lg text-sm font-medium transition-all duration-200 ${
                    mode === 'llm' 
                      ? 'bg-[var(--color-primary)] text-white shadow-lg shadow-blue-500/25' 
                      : 'text-[var(--color-text-muted)] hover:text-white hover:bg-white/5'
                  }`}
                >
                  AI Copilot
                </button>
                <button
                  onClick={() => setMode('deterministic')}
                  className={`flex-1 py-2.5 px-4 rounded-lg text-sm font-medium transition-all duration-200 ${
                    mode === 'deterministic' 
                      ? 'bg-[var(--color-primary)] text-white shadow-lg shadow-blue-500/25' 
                      : 'text-[var(--color-text-muted)] hover:text-white hover:bg-white/5'
                  }`}
                >
                  Auto-Clean
                </button>
              </div>

              <form onSubmit={handleSubmit} className="space-y-6">
                
                {/* File Upload Area */}
                <div className="space-y-2">
                  <label className="text-sm font-medium text-slate-300">Dataset File (CSV)</label>
                  <div 
                    onClick={() => fileInputRef.current?.click()}
                    onDragOver={handleDragOver}
                    onDrop={handleDrop}
                    className={`relative group cursor-pointer border-2 border-dashed rounded-xl p-8 text-center transition-all duration-200 ${
                      file 
                        ? 'border-[var(--color-primary)] bg-blue-500/5' 
                        : 'border-slate-600 hover:border-slate-400 bg-slate-800/30'
                    }`}
                  >
                    <input 
                      type="file" 
                      ref={fileInputRef}
                      onChange={handleFileChange}
                      accept=".csv"
                      className="hidden" 
                    />
                    
                    {file ? (
                      <div className="space-y-3">
                        <div className="mx-auto w-12 h-12 rounded-full bg-blue-500/20 flex items-center justify-center">
                          <FileType className="w-6 h-6 text-[var(--color-primary)]" />
                        </div>
                        <div>
                          <p className="text-sm font-semibold text-white">{file.name}</p>
                          <p className="text-xs text-slate-400 mt-1">
                            {(file.size / 1024 / 1024).toFixed(2)} MB
                          </p>
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        <div className="mx-auto w-12 h-12 rounded-full bg-slate-800 flex items-center justify-center group-hover:scale-110 transition-transform">
                          <Upload className="w-6 h-6 text-slate-400 group-hover:text-white" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-slate-300">Click to upload or drag and drop</p>
                          <p className="text-xs text-slate-500 mt-1">CSV files only</p>
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* AI Goal Input */}
                {mode === 'llm' && (
                  <div className="space-y-2 animate-in fade-in slide-in-from-bottom-2">
                    <label className="text-sm font-medium text-slate-300">Transformation Goal</label>
                    <textarea
                      value={goal}
                      onChange={(e) => setGoal(e.target.value)}
                      placeholder="e.g., Remove rows with missing ages, calculate average revenue, and generate a pie chart..."
                      rows={3}
                      className="w-full bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all resize-none"
                    />
                  </div>
                )}

                <button
                  type="submit"
                  disabled={isLoading || !file}
                  className="w-full relative group overflow-hidden bg-[var(--color-primary)] text-white rounded-xl font-medium py-3.5 px-4 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-[var(--color-primary-hover)] hover:shadow-lg hover:shadow-blue-500/25 active:scale-[0.98]"
                >
                  <div className="absolute inset-0 w-full h-full bg-gradient-to-r from-white/0 via-white/10 to-white/0 -translate-x-full group-hover:animate-[shimmer_1.5s_infinite]" />
                  <span className="relative flex items-center justify-center gap-2">
                    {isLoading ? (
                      <>
                        <svg className="animate-spin -ml-1 mr-2 h-5 w-5 text-white" fill="none" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        Processing Data...
                      </>
                    ) : (
                      <>
                        <Play className="w-5 h-5 fill-current" />
                        {mode === 'llm' ? 'Run AI Pipeline' : 'Run Deterministic Clean'}
                      </>
                    )}
                  </span>
                </button>
              </form>
            </div>
          </div>

          {/* Results/Preview Column */}
          <div className="lg:col-span-7 space-y-6">
            
            {error && <Alert type="error" message={error} />}
            {result && !error && <Alert type="success" message="Transformation completed successfully!" />}
            
            {/* Empty State */}
            {!result && !isLoading && !error && (
              <div className="h-full min-h-[400px] glass-panel flex flex-col items-center justify-center p-8 text-center border-dashed border-2 border-slate-700/50">
                <div className="w-20 h-20 rounded-full bg-slate-800/50 flex items-center justify-center mb-6 border border-white/5">
                  <BarChart2 className="w-10 h-10 text-slate-600" />
                </div>
                <h3 className="text-xl font-semibold text-slate-300 mb-2">No Data Processed Yet</h3>
                <p className="text-slate-500 max-w-sm">
                  Upload a CSV and run the ETL pipeline to see your processed data preview and artifacts here.
                </p>
              </div>
            )}

            {/* Loading State Outline */}
            {isLoading && (
              <div className="h-full min-h-[400px] glass-panel p-6 animate-pulse">
                <div className="flex gap-4 mb-8">
                  <div className="h-10 bg-slate-800 rounded-lg w-32"></div>
                  <div className="h-10 bg-slate-800 rounded-lg w-40"></div>
                </div>
                <div className="space-y-4">
                  <div className="h-12 bg-slate-800 rounded-lg w-full"></div>
                  <div className="h-12 bg-slate-800 rounded-lg w-full"></div>
                  <div className="h-12 bg-slate-800 rounded-lg w-full"></div>
                  <div className="h-12 bg-slate-800 rounded-lg w-full"></div>
                </div>
              </div>
            )}

            {/* Results Display */}
            {result && !isLoading && (
              <div className="animate-in fade-in slide-in-from-right-8 duration-500">
                
                {/* Visual Artifacts */}
                {result.artifacts && Object.keys(result.artifacts).length > 0 && (
                  <div className="mb-6 grid grid-cols-2 sm:grid-cols-3 gap-4">
                    {Object.entries(result.artifacts).map(([filename, url]) => (
                      <a
                        key={filename}
                        href={`http://localhost:8000${url}`}
                        target="_blank"
                        rel="noreferrer"
                        className="glass-panel p-4 flex flex-col items-center justify-center text-center hover:bg-white/5 transition-colors group cursor-pointer"
                      >
                        <div className="w-10 h-10 rounded-full bg-blue-500/20 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                          <BarChart2 className="w-5 h-5 text-[var(--color-primary)]" />
                        </div>
                        <span className="text-xs font-medium text-slate-300 capitalize">
                          {filename.replace(/_/g, ' ').replace('.html', '')}
                        </span>
                      </a>
                    ))}
                  </div>
                )}

                {/* Warnings Display */}
                {result.warnings && result.warnings.length > 0 && (
                  <div className="mb-6 space-y-3">
                    {result.warnings.map((warn: string, i: number) => (
                      <Alert key={i} type="info" message={warn} />
                    ))}
                  </div>
                )}

                {/* Download Actions */}
                <div className="flex flex-wrap gap-4 mb-6">
                  {result.download_url && (
                    <a
                      href={`http://localhost:8000${result.download_url}`}
                      className="inline-flex items-center justify-center px-4 py-2 bg-slate-800 hover:bg-slate-700 text-sm font-medium rounded-lg text-white border border-slate-600 transition-colors shadow-sm"
                    >
                      Download Processed CSV
                    </a>
                  )}
                  {result.download_cleaned_url && (
                    <a
                      href={`http://localhost:8000${result.download_cleaned_url}`}
                      className="inline-flex items-center justify-center px-4 py-2 bg-slate-800 hover:bg-slate-700 text-sm font-medium rounded-lg text-white border border-slate-600 transition-colors shadow-sm"
                    >
                      Download Cleaned CSV
                    </a>
                  )}
                  {result.download_report_url && (
                    <a
                      href={`http://localhost:8000${result.download_report_url}`}
                      className="inline-flex items-center justify-center px-4 py-2 bg-slate-800 hover:bg-slate-700 text-sm font-medium rounded-lg text-white border border-slate-600 transition-colors shadow-sm"
                    >
                      Download QA Report (JSON)
                    </a>
                  )}
                </div>

                {/* Data Preview Table */}
                {previewData ? (
                  <div className="glass-panel overflow-hidden flex flex-col">
                    <div className="px-6 py-4 border-b border-slate-700/50 bg-slate-900/50 flex justify-between items-center">
                      <h3 className="text-sm font-medium text-white">Data Preview (Top 100 Rows)</h3>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm text-left">
                        <thead className="text-xs text-slate-400 bg-slate-800/50 uppercase border-b border-slate-700">
                          <tr>
                            {previewData.headers.map((header, i) => (
                              <th key={i} className="px-6 py-3 font-medium whitespace-nowrap">
                                {header}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/50">
                          {previewData.rows.map((row, i) => (
                            <tr key={i} className="hover:bg-white/[0.02] transition-colors">
                              {row.map((cell, j) => (
                                <td key={j} className="px-6 py-4 whitespace-nowrap text-slate-300">
                                  {cell !== null && cell !== undefined && cell !== '' ? String(cell) : <span className="text-slate-600 italic">null</span>}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <div className="glass-panel p-8 text-center">
                    <p className="text-slate-400">Loading data preview...</p>
                  </div>
                )}
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}

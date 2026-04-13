import React, { useState, useRef } from 'react';
import Papa from 'papaparse';
import {
  Upload, FileType, CheckCircle2, Play, AlertCircle, BarChart2,
  MessageSquare, FileText, Download, Activity, Database, Eye, Send, Loader2, TrendingUp, Clock
} from 'lucide-react';
import {
  ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid,
  BarChart, Bar, PieChart, Pie, Cell, Legend, LineChart, Line,
} from 'recharts';
import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL || '';
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY || '';
const supabase = createClient(supabaseUrl, supabaseAnonKey);

const API = '';

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
    <div className={`flex items-center gap-3 p-4 rounded-lg border ${colors[type]} mb-4 animate-in fade-in slide-in-from-top-4`}>
      <Icon className="w-5 h-5 flex-shrink-0" />
      <p className="text-sm font-medium">{message}</p>
    </div>
  );
};

// Tab definitions
const TABS = [
  { id: 'preview', label: 'Preview', icon: Eye },
  { id: 'quality', label: 'Data Quality', icon: Activity },
  { id: 'summary', label: 'Summary', icon: Database },
  { id: 'viz', label: 'Visualizations', icon: TrendingUp },
  { id: 'chat', label: 'Chat', icon: MessageSquare },
  { id: 'report', label: 'Report', icon: FileText },
  { id: 'log', label: 'Transform Log', icon: BarChart2 },
  { id: 'history', label: 'History', icon: Clock },
  { id: 'export', label: 'Export', icon: Download },
] as const;

type TabId = typeof TABS[number]['id'];

export default function App() {
  const [session, setSession] = useState<any>(null);
  const [user, setUser] = useState<any>(null);
  const [historyData, setHistoryData] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  React.useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setUser(session?.user ?? null);
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
      setUser(session?.user ?? null);
    });
    return () => subscription.unsubscribe();
  }, []);

  const handleLogin = async () => {
    await supabase.auth.signInWithOAuth({ provider: 'google' });
  };

  const handleLogout = async () => {
    await supabase.auth.signOut();
  };

  const getHeaders = () => {
    const headers: Record<string, string> = {};
    if (session?.access_token) {
      headers['Authorization'] = `Bearer ${session.access_token}`;
    }
    return headers;
  };

  const [file, setFile] = useState<File | null>(null);
  const [goal, setGoal] = useState('');
  const [mode, setMode] = useState<'llm' | 'deterministic'>('llm');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<any>(null);
  const [previewData, setPreviewData] = useState<{ headers: string[], rows: any[][] } | null>(null);
  const [activeTab, setActiveTab] = useState<TabId>('preview');

  // New feature states
  const [qualityData, setQualityData] = useState<any>(null);
  const [summaryData, setSummaryData] = useState<any>(null);
  const [dashboardData, setDashboardData] = useState<any>(null);
  const [chatMessages, setChatMessages] = useState<{ role: string; text: string }[]>([]);
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  const [reportData, setReportData] = useState<any>(null);
  const [reportLoading, setReportLoading] = useState(false);
  const [vizLoading, setVizLoading] = useState(false);
  const [qualityLoading, setQualityLoading] = useState(false);
  const [summaryLoading, setSummaryLoading] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const reportRef = useRef<HTMLDivElement>(null);

  const downloadDashboardPDF = async () => {
    if (!reportRef.current) return;
    try {
      const element = reportRef.current;
      const canvas = await html2canvas(element, { 
        backgroundColor: '#0f172a',
        scale: 2 // Higher quality
      });
      const imgData = canvas.toDataURL('image/png');
      const pdf = new jsPDF('p', 'mm', 'a4');
      const pdfWidth = pdf.internal.pageSize.getWidth();
      const pdfHeight = (canvas.height * pdfWidth) / canvas.width;
      
      pdf.addImage(imgData, 'PNG', 0, 0, pdfWidth, pdfHeight);
      pdf.save(`DIANA_Dashboard_${Date.now()}.pdf`);
    } catch (err) {
      console.error("Failed to generate PDF snapshot", err);
    }
  };

  // The filename of the cleaned/processed file for use with endpoints
  const getResultFilename = () => {
    if (!result) return null;
    if (result.output_filename) return result.output_filename;
    if (result.download_cleaned_url) return result.download_cleaned_url.split('/').pop();
    return null;
  };

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
      const validExts = ['.csv', '.xlsx', '.xls', '.json'];
      const ext = '.' + droppedFile.name.split('.').pop()?.toLowerCase();
      if (validExts.includes(ext)) {
        setFile(droppedFile);
        setError(null);
      } else {
        setError("Please upload a CSV, Excel (.xlsx/.xls), or JSON file.");
      }
    }
  };

  const loadCsvPreview = async (url: string) => {
    try {
      const response = await fetch(`${API}${url}`);
      if (!response.ok) throw new Error("Failed to fetch resulting CSV");
      const csvText = await response.text();
      Papa.parse(csvText, {
        header: true,
        preview: 100,
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
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file) { setError("Please select a file first."); return; }
    if (mode === 'llm' && !goal.trim()) { setError("Please enter a transformation goal."); return; }

    setIsLoading(true);
    setError(null);
    setResult(null);
    setPreviewData(null);
    setQualityData(null);
    setSummaryData(null);
    setDashboardData(null);
    setChatMessages([]);
    setReportData(null);
    setActiveTab('preview');

    const formData = new FormData();
    formData.append('file', file);
    if (mode === 'llm') formData.append('goal', goal);

    const endpoint = mode === 'llm' ? `${API}/process` : `${API}/transform`;

    try {
      const response = await fetch(endpoint, { method: 'POST', body: formData, headers: getHeaders() });
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || data.error || "An error occurred.");
      setResult(data);
      const downloadUrl = data.download_url || data.download_cleaned_url;
      if (downloadUrl) await loadCsvPreview(downloadUrl);
      // Auto-refresh history after successful transformation
      if (user) loadHistory();
    } catch (err: any) {
      setError(err.message || "Failed to connect. Is the backend running?");
    } finally {
      setIsLoading(false);
    }
  };

  // Load data quality info
  const loadQuality = async () => {
    const fn = getResultFilename();
    if (!fn) return;
    setQualityLoading(true);
    try {
      const res = await fetch(`${API}/data-quality?filename=${encodeURIComponent(fn)}`, { headers: getHeaders() });
      const data = await res.json();
      setQualityData(data);
    } catch (err) {
      console.error(err);
    } finally {
      setQualityLoading(false);
    }
  };

  // Load data summary
  const loadSummary = async () => {
    const fn = getResultFilename();
    if (!fn) return;
    setSummaryLoading(true);
    try {
      const res = await fetch(`${API}/data-summary?filename=${encodeURIComponent(fn)}`, { headers: getHeaders() });
      const data = await res.json();
      setSummaryData(data);
    } catch (err) {
      console.error(err);
    } finally {
      setSummaryLoading(false);
    }
  };

  // Load dashboard visualizations
  const loadViz = async () => {
    const fn = getResultFilename();
    if (!fn) return;
    setVizLoading(true);
    try {
      const res = await fetch(`${API}/dashboard-data?filename=${encodeURIComponent(fn)}`, { headers: getHeaders() });
      const data = await res.json();
      setDashboardData(data);
    } catch (err) {
      console.error(err);
    } finally {
      setVizLoading(false);
    }
  };

  // Load user history
  const loadHistory = async () => {
    if (!user) return;
    setHistoryLoading(true);
    try {
      const res = await fetch(`${API}/history`, { headers: getHeaders() });
      if (!res.ok) throw new Error("Failed to load history");
      const data = await res.json();
      setHistoryData(data.history || []);
    } catch (err) {
      console.error(err);
    } finally {
      setHistoryLoading(false);
    }
  };

  // Handle tab click with lazy loading
  const handleTabClick = (tab: TabId) => {
    setActiveTab(tab);
    if (tab === 'quality' && !qualityData && !qualityLoading) loadQuality();
    if (tab === 'summary' && !summaryData && !summaryLoading) loadSummary();
    if (tab === 'viz' && !dashboardData && !vizLoading) loadViz();
    if (tab === 'history' && !historyLoading) loadHistory();
  };

  // Chat handler
  const sendChat = async () => {
    const fn = getResultFilename();
    if (!fn || !chatInput.trim()) return;
    const question = chatInput.trim();
    setChatMessages(prev => [...prev, { role: 'user', text: question }]);
    setChatInput('');
    setChatLoading(true);
    try {
      const res = await fetch(`${API}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
        body: JSON.stringify({ filename: fn, question }),
      });
      const data = await res.json();
      setChatMessages(prev => [...prev, { role: 'assistant', text: data.answer }]);
    } catch (err) {
      setChatMessages(prev => [...prev, { role: 'assistant', text: 'Error connecting to AI. Please try again.' }]);
    } finally {
      setChatLoading(false);
    }
  };

  // Report generator
  const generateReport = async () => {
    const fn = getResultFilename();
    if (!fn) return;
    setReportLoading(true);
    try {
      const res = await fetch(`${API}/generate-report?filename=${encodeURIComponent(fn)}`, { method: 'POST', headers: getHeaders() });
      const data = await res.json();
      setReportData(data);
    } catch (err) {
      console.error(err);
    } finally {
      setReportLoading(false);
    }
  };

  // -----------------------------------------------------------------------
  // Render helpers for each tab
  // -----------------------------------------------------------------------

  const renderQualityTab = () => {
    if (qualityLoading) return <div className="flex items-center justify-center p-12"><Loader2 className="w-8 h-8 animate-spin text-blue-400" /></div>;
    if (!qualityData) return <p className="text-slate-400 p-8 text-center">Click to load data quality analysis</p>;
    return (
      <div className="space-y-6 p-6">
        {/* Summary Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-slate-800/60 rounded-xl p-4 border border-slate-700/50">
            <p className="text-xs text-slate-400 uppercase tracking-wider">Rows</p>
            <p className="text-2xl font-bold text-white mt-1">{qualityData.rows?.toLocaleString()}</p>
          </div>
          <div className="bg-slate-800/60 rounded-xl p-4 border border-slate-700/50">
            <p className="text-xs text-slate-400 uppercase tracking-wider">Columns</p>
            <p className="text-2xl font-bold text-white mt-1">{qualityData.columns}</p>
          </div>
          <div className={`bg-slate-800/60 rounded-xl p-4 border ${qualityData.total_missing > 0 ? 'border-amber-500/30' : 'border-green-500/30'}`}>
            <p className="text-xs text-slate-400 uppercase tracking-wider">Missing Values</p>
            <p className={`text-2xl font-bold mt-1 ${qualityData.total_missing > 0 ? 'text-amber-400' : 'text-green-400'}`}>
              {qualityData.total_missing?.toLocaleString()}
            </p>
          </div>
          <div className={`bg-slate-800/60 rounded-xl p-4 border ${qualityData.duplicate_rows > 0 ? 'border-red-500/30' : 'border-green-500/30'}`}>
            <p className="text-xs text-slate-400 uppercase tracking-wider">Duplicates</p>
            <p className={`text-2xl font-bold mt-1 ${qualityData.duplicate_rows > 0 ? 'text-red-400' : 'text-green-400'}`}>
              {qualityData.duplicate_rows}
            </p>
          </div>
        </div>

        {/* Missing Values per Column */}
        {qualityData.missing_values && Object.keys(qualityData.missing_values).length > 0 && (
          <div className="bg-slate-800/40 rounded-xl p-5 border border-slate-700/50">
            <h4 className="text-sm font-semibold text-white mb-4">Missing Values by Column</h4>
            <div className="space-y-3 max-h-60 overflow-y-auto">
              {Object.entries(qualityData.missing_values).map(([col, count]) => {
                const pct = qualityData.missing_percent?.[col] || 0;
                return (
                  <div key={col} className="flex items-center gap-3">
                    <span className="text-xs text-slate-300 w-32 truncate" title={col}>{col}</span>
                    <div className="flex-1 bg-slate-700/50 rounded-full h-2.5 overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${Number(pct) > 50 ? 'bg-red-500' : Number(pct) > 10 ? 'bg-amber-500' : 'bg-green-500'}`}
                        style={{ width: `${Math.min(100, Number(pct))}%` }}
                      />
                    </div>
                    <span className="text-xs text-slate-400 w-20 text-right">{String(count)} ({Number(pct).toFixed(1)}%)</span>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Outliers */}
        {qualityData.outliers_per_column && Object.keys(qualityData.outliers_per_column).length > 0 && (
          <div className="bg-slate-800/40 rounded-xl p-5 border border-slate-700/50">
            <h4 className="text-sm font-semibold text-white mb-4">Outliers Detected (IQR Method)</h4>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              {Object.entries(qualityData.outliers_per_column).map(([col, count]) => (
                <div key={col} className="flex justify-between items-center bg-slate-900/50 rounded-lg px-3 py-2">
                  <span className="text-xs text-slate-300 truncate">{col}</span>
                  <span className={`text-xs font-semibold ${Number(count) > 0 ? 'text-amber-400' : 'text-green-400'}`}>{String(count)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Column Types */}
        {qualityData.column_types && (
          <div className="bg-slate-800/40 rounded-xl p-5 border border-slate-700/50">
            <h4 className="text-sm font-semibold text-white mb-4">Column Types</h4>
            <div className="flex flex-wrap gap-2">
              {Object.entries(qualityData.column_types).map(([col, dtype]) => (
                <span key={col} className="inline-flex items-center gap-1.5 text-xs bg-slate-900/60 border border-slate-600/40 rounded-full px-3 py-1.5">
                  <span className="text-slate-300">{col}</span>
                  <span className="text-blue-400 font-mono">{String(dtype)}</span>
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderSummaryTab = () => {
    if (summaryLoading) return <div className="flex items-center justify-center p-12"><Loader2 className="w-8 h-8 animate-spin text-blue-400" /></div>;
    if (!summaryData) return <p className="text-slate-400 p-8 text-center">Click to load data summary</p>;
    return (
      <div className="p-6 space-y-4">
        <div className="flex gap-4 mb-4">
          <div className="bg-slate-800/60 rounded-xl px-4 py-2 border border-slate-700/50">
            <span className="text-xs text-slate-400">Rows: </span><span className="text-white font-bold">{summaryData.rows?.toLocaleString()}</span>
          </div>
          <div className="bg-slate-800/60 rounded-xl px-4 py-2 border border-slate-700/50">
            <span className="text-xs text-slate-400">Columns: </span><span className="text-white font-bold">{summaryData.columns}</span>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="text-xs text-slate-400 bg-slate-800/50 uppercase border-b border-slate-700">
              <tr>
                <th className="px-4 py-3">Column</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Non-Null</th>
                <th className="px-4 py-3">Null</th>
                <th className="px-4 py-3">Unique</th>
                <th className="px-4 py-3">Mean</th>
                <th className="px-4 py-3">Median</th>
                <th className="px-4 py-3">Std</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {summaryData.column_info?.map((col: any, i: number) => (
                <tr key={i} className="hover:bg-white/[0.02]">
                  <td className="px-4 py-3 text-white font-medium">{col.name}</td>
                  <td className="px-4 py-3 text-blue-400 font-mono text-xs">{col.dtype}</td>
                  <td className="px-4 py-3 text-slate-300">{col.non_null}</td>
                  <td className={`px-4 py-3 ${col.null_count > 0 ? 'text-amber-400' : 'text-green-400'}`}>{col.null_count}</td>
                  <td className="px-4 py-3 text-slate-300">{col.unique}</td>
                  <td className="px-4 py-3 text-slate-300">{col.mean !== undefined ? col.mean : '—'}</td>
                  <td className="px-4 py-3 text-slate-300">{col['50%'] !== undefined ? col['50%'] : '—'}</td>
                  <td className="px-4 py-3 text-slate-300">{col.std !== undefined ? col.std : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const darkTooltipStyle = { backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px', color: '#e2e8f0', fontSize: '12px' };

  const renderChartCard = (chart: any) => {
    const cardClass = "bg-slate-900/80 rounded-2xl border border-slate-700/40 p-5 flex flex-col";
    const titleBlock = (
      <div className="mb-4">
        <h4 className="text-sm font-bold text-white">{chart.title}</h4>
        {chart.subtitle && <p className="text-xs text-slate-500 mt-0.5">{chart.subtitle}</p>}
      </div>
    );

    if (chart.type === 'area') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={chart.data}>
              <defs>
                <linearGradient id={`grad_${chart.id}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={chart.color} stopOpacity={0.4} />
                  <stop offset="95%" stopColor={chart.color} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey={chart.xKey} tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={darkTooltipStyle} />
              <Area type="monotone" dataKey={chart.dataKey} stroke={chart.color} fill={`url(#grad_${chart.id})`} strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'bar') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey={chart.data?.[0]?.range !== undefined ? 'range' : 'name'} tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} angle={-30} textAnchor="end" height={50} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={darkTooltipStyle} />
              <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                {chart.data.map((entry: any, idx: number) => (
                  <Cell key={idx} fill={entry.color || chart.color || '#3b82f6'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'horizontal_bar') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={Math.max(200, chart.data.length * 28)}>
            <BarChart data={chart.data} layout="vertical" margin={{ left: 10, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} width={90} />
              <Tooltip contentStyle={darkTooltipStyle} />
              <Bar dataKey="value" radius={[0, 4, 4, 0]} barSize={16}>
                {chart.data.map((entry: any, idx: number) => (
                  <Cell key={idx} fill={entry.color || '#3b82f6'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'donut') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={chart.data} cx="50%" cy="50%" innerRadius={55} outerRadius={85} paddingAngle={3} dataKey="value" nameKey="name" stroke="none">
                {chart.data.map((entry: any, idx: number) => (
                  <Cell key={idx} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip contentStyle={darkTooltipStyle} formatter={(value: any, name: any, props: any) => [`${value} (${props.payload.percent}%)`, name]} />
              <Legend iconType="circle" iconSize={8} formatter={(value: string) => <span style={{ color: '#94a3b8', fontSize: '11px' }}>{value}</span>} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'line') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey={chart.xKey} tick={{ fill: '#64748b', fontSize: 9 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={darkTooltipStyle} />
              <Line type="monotone" dataKey={chart.dataKey} stroke={chart.color} strokeWidth={2} dot={{ fill: chart.color, r: 3 }} activeDot={{ r: 5 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'grouped_bar') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <ResponsiveContainer width="100%" height={Math.max(220, chart.data.length * 30)}>
            <BarChart data={chart.data} layout="vertical" margin={{ left: 10, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} width={90} />
              <Tooltip contentStyle={darkTooltipStyle} />
              <Legend iconType="circle" iconSize={8} formatter={(v: string) => <span style={{ color: '#94a3b8', fontSize: '11px' }}>{v}</span>} />
              {chart.keys.map((key: string, idx: number) => (
                <Bar key={key} dataKey={key} fill={chart.colors[idx]} radius={[0, 4, 4, 0]} barSize={14} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (chart.type === 'ranked_list') {
      return (
        <div className={cardClass} key={chart.id}>
          {titleBlock}
          <div className="space-y-3 mt-1">
            {chart.data.map((item: any) => (
              <div key={item.rank} className="flex items-center gap-3">
                <span className="text-xs font-bold text-slate-400 w-5">{item.rank}.</span>
                <div className="flex-1 min-w-0">
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-sm font-medium text-white truncate">{item.name}</span>
                    <span className="text-xs text-slate-400 ml-2 whitespace-nowrap">{item.value} ({item.percent}%)</span>
                  </div>
                  <div className="w-full bg-slate-800 rounded-full h-2 overflow-hidden">
                    <div className="h-full rounded-full transition-all duration-700" style={{ width: `${item.percent}%`, backgroundColor: item.color }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      );
    }

    return null;
  };

  const renderVizTab = () => {
    if (vizLoading) return <div className="flex items-center justify-center p-12"><Loader2 className="w-8 h-8 animate-spin text-blue-400" /></div>;
    if (!dashboardData) return <p className="text-slate-400 p-8 text-center">Click to load auto-generated visualizations</p>;
    if (!dashboardData.charts || dashboardData.charts.length === 0) return <p className="text-slate-400 p-8 text-center">No visualizations could be generated for this dataset.</p>;
    return (
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">Dashboard ({dashboardData.charts.length} charts)</h4>
          <button onClick={() => { setDashboardData(null); loadViz(); }} className="text-xs text-blue-400 hover:text-blue-300 transition-colors">Refresh ↻</button>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {dashboardData.charts.map((chart: any) => renderChartCard(chart))}
        </div>
      </div>
    );
  };

  const renderChatTab = () => (
    <div className="flex flex-col h-[500px]">
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {chatMessages.length === 0 && (
          <div className="text-center py-12">
            <MessageSquare className="w-12 h-12 text-slate-600 mx-auto mb-4" />
            <h4 className="text-lg font-semibold text-slate-300 mb-2">Chat with your Dataset</h4>
            <p className="text-sm text-slate-500 max-w-md mx-auto">
              Ask questions like "What is the average salary?", "How many rows have missing values?", or "What's the correlation between age and income?"
            </p>
          </div>
        )}
        {chatMessages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[80%] rounded-2xl px-4 py-3 text-sm ${
              msg.role === 'user'
                ? 'bg-blue-600 text-white rounded-br-md'
                : 'bg-slate-800 text-slate-200 border border-slate-700/50 rounded-bl-md'
            }`}>
              <p className="whitespace-pre-wrap">{msg.text}</p>
            </div>
          </div>
        ))}
        {chatLoading && (
          <div className="flex justify-start">
            <div className="bg-slate-800 text-slate-400 rounded-2xl rounded-bl-md px-4 py-3 border border-slate-700/50">
              <Loader2 className="w-4 h-4 animate-spin" />
            </div>
          </div>
        )}
      </div>
      <div className="border-t border-slate-700/50 p-4">
        <div className="flex gap-3">
          <input
            type="text"
            value={chatInput}
            onChange={(e) => setChatInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !chatLoading && sendChat()}
            placeholder="Ask a question about your data..."
            className="flex-1 bg-slate-900 border border-slate-700 rounded-xl px-4 py-3 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all"
          />
          <button
            onClick={sendChat}
            disabled={chatLoading || !chatInput.trim()}
            className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white rounded-xl px-4 py-3 transition-colors"
          >
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );

  const renderReportTab = () => (
    <div className="p-6 space-y-6">
      {!reportData && !reportLoading && (
        <div className="text-center py-12">
          <FileText className="w-12 h-12 text-slate-600 mx-auto mb-4" />
          <h4 className="text-lg font-semibold text-slate-300 mb-2">AI Data Analysis Report</h4>
          <p className="text-sm text-slate-500 max-w-md mx-auto mb-6">
            Generate a comprehensive professional report with insights, quality analysis, and recommendations powered by AI.
          </p>
          <button
            onClick={generateReport}
            className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white rounded-xl font-medium py-3 px-8 transition-all shadow-lg shadow-blue-500/25 active:scale-[0.98]"
          >
            <span className="flex items-center gap-2">
              <FileText className="w-5 h-5" />
              Generate AI Report
            </span>
          </button>
        </div>
      )}
      {reportLoading && (
        <div className="text-center py-12">
          <Loader2 className="w-10 h-10 animate-spin text-blue-400 mx-auto mb-4" />
          <p className="text-slate-400">Generating your AI report... This may take a moment.</p>
        </div>
      )}
      {reportData && (
        <div className="space-y-6">
          <div className="flex flex-wrap gap-3">
            {reportData.html_url && (
              <a href={`${API}${reportData.html_url}`} target="_blank" rel="noreferrer"
                 className="inline-flex items-center gap-2 px-4 py-2.5 bg-blue-600/20 hover:bg-blue-600/30 text-blue-400 rounded-xl text-sm font-medium border border-blue-500/30 transition-colors">
                <Eye className="w-4 h-4" /> View Backend Report
              </a>
            )}
            <button
               onClick={downloadDashboardPDF}
               className="inline-flex items-center gap-2 px-4 py-2.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-sm font-medium border border-indigo-500/30 transition-colors shadow-lg shadow-indigo-500/25">
              <Download className="w-4 h-4" /> Download Dashboard PDF
            </button>
            <button onClick={generateReport}
                    className="inline-flex items-center gap-2 px-4 py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl text-sm font-medium border border-slate-600 transition-colors">
              Regenerate Text Options
            </button>
          </div>
          
          {/* Printable Combined Container */}
          <div ref={reportRef} className="bg-slate-900/50 p-6 rounded-2xl border border-slate-700/50 space-y-8">
            <div className="border-b border-slate-700/50 pb-4 mb-6">
              <h2 className="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-400">
                Data Analysis Dashboard
              </h2>
              <p className="text-slate-400 text-sm mt-1">Generated by DIANA AI</p>
            </div>
            
            {/* The Text Report */}
            {reportData.report_text && (
              <div className="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
                <h3 className="text-lg font-semibold text-white mb-4">AI Analysis</h3>
                <pre className="text-sm text-slate-300 whitespace-pre-wrap font-sans leading-relaxed">{reportData.report_text}</pre>
              </div>
            )}

            {/* The Recharts Dashboard Array */}
            {dashboardData?.charts && dashboardData.charts.length > 0 && (
              <div>
                <h3 className="text-lg font-semibold text-white mb-4 mt-8">Auto-Generated Visualizations</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                  {dashboardData.charts.map((chart: any) => renderChartCard(chart))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );

  const renderLogTab = () => {
    const log = result?.transformation_log;
    if (!log || log.length === 0) return <p className="text-slate-400 p-8 text-center">No transformation log available.</p>;
    return (
      <div className="p-6">
        <h4 className="text-sm font-semibold text-white mb-4">Transformation Steps</h4>
        <div className="relative">
          <div className="absolute left-[18px] top-0 bottom-0 w-0.5 bg-slate-700/60" />
          <div className="space-y-4">
            {log.map((entry: any, i: number) => (
              <div key={i} className="relative flex items-start gap-4 pl-10">
                <div className="absolute left-2.5 top-1 w-3.5 h-3.5 rounded-full bg-blue-500/30 border-2 border-blue-500 flex items-center justify-center">
                  <div className="w-1.5 h-1.5 rounded-full bg-blue-400" />
                </div>
                <div className="flex-1 bg-slate-800/40 rounded-xl p-4 border border-slate-700/50">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-mono text-blue-400">Step {entry.step}</span>
                    {entry.rows_affected > 0 && (
                      <span className="text-xs text-amber-400 bg-amber-500/10 px-2 py-0.5 rounded-full">
                        {entry.rows_affected} rows affected
                      </span>
                    )}
                  </div>
                  <p className="text-sm text-slate-200 mt-1">{entry.action}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const renderHistoryTab = () => {
    if (!user) return (
      <div className="text-center p-12">
        <Clock className="w-12 h-12 text-slate-600 mx-auto mb-4" />
        <h4 className="text-lg font-semibold text-slate-300 mb-2">Login Required</h4>
        <p className="text-slate-400">Please login to view your transformation history across devices.</p>
        <button onClick={handleLogin} className="mt-4 bg-blue-600 hover:bg-blue-500 px-6 py-2 rounded-lg text-white font-medium shadow-lg transition-colors">Log in with Google</button>
      </div>
    );
    if (historyLoading) return <div className="flex items-center justify-center p-12"><Loader2 className="w-8 h-8 animate-spin text-blue-400" /></div>;
    if (historyData.length === 0) return <p className="text-slate-400 p-8 text-center">No history found. Try uploading and transforming a file!</p>;
    
    return (
      <div className="p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h4 className="text-sm font-semibold text-white">Your Past Transformations</h4>
          <button onClick={() => loadHistory()} className="text-xs text-blue-400 hover:text-blue-300 transition-colors">Refresh ↻</button>
        </div>
        <div className="space-y-4 max-h-[500px] overflow-y-auto pr-2 custom-scrollbar">
          {historyData.map((item: any) => (
            <div key={item.id} className="bg-slate-800/40 rounded-xl p-4 border border-slate-700/50 hover:bg-slate-800/60 transition-colors">
               <div className="flex justify-between items-center mb-2">
                 <span className="text-blue-400 font-medium text-sm truncate pr-4">{item.original_filename}</span>
                 <span className="text-slate-500 text-xs whitespace-nowrap">{new Date(item.timestamp).toLocaleString()}</span>
               </div>
               <div className="flex gap-2 mt-3 flex-wrap">
                 {item.cleaned_file_url && <a href={item.cleaned_file_url} target="_blank" rel="noreferrer" className="text-xs bg-slate-700/50 hover:bg-slate-600 px-3 py-1.5 rounded-lg text-slate-300 transition block">Download Clean Data</a>}
                 {item.report_file_url && <a href={item.report_file_url} target="_blank" rel="noreferrer" className="text-xs bg-indigo-600/20 hover:bg-indigo-600/40 border border-indigo-500/30 px-3 py-1.5 rounded-lg text-indigo-300 transition block">View Report</a>}
               </div>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const renderExportTab = () => {
    const fn = getResultFilename();
    if (!fn) return <p className="text-slate-400 p-8 text-center">Process a dataset first to enable exports.</p>;
    const formats = [
      { id: 'csv', label: 'CSV', desc: 'Comma-separated values', icon: '📄', color: 'from-green-500/20 to-green-600/20 border-green-500/30' },
      { id: 'excel', label: 'Excel', desc: 'Microsoft Excel (.xlsx)', icon: '📊', color: 'from-emerald-500/20 to-emerald-600/20 border-emerald-500/30' },
      { id: 'json', label: 'JSON', desc: 'JavaScript Object Notation', icon: '🔧', color: 'from-amber-500/20 to-amber-600/20 border-amber-500/30' },
      { id: 'pdf', label: 'PDF', desc: 'Portable Document Format', icon: '📋', color: 'from-red-500/20 to-red-600/20 border-red-500/30' },
    ];
    return (
      <div className="p-6">
        <h4 className="text-sm font-semibold text-white mb-4">Export Processed Data</h4>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {formats.map(fmt => (
            <a
              key={fmt.id}
              href={`${API}/export/${encodeURIComponent(fn)}/${fmt.id}`}
              className={`group bg-gradient-to-br ${fmt.color} rounded-xl p-5 border text-center hover:scale-[1.03] transition-all cursor-pointer block`}
            >
              <div className="text-3xl mb-3">{fmt.icon}</div>
              <p className="text-sm font-semibold text-white">{fmt.label}</p>
              <p className="text-xs text-slate-400 mt-1">{fmt.desc}</p>
            </a>
          ))}
        </div>
      </div>
    );
  };

  // -----------------------------------------------------------------------
  // Main render
  // -----------------------------------------------------------------------

  return (
    <div className="min-h-screen bg-[var(--color-background)] text-[var(--color-text-main)] py-12 px-4 sm:px-6 lg:px-8 bg-[radial-gradient(ellipse_at_top,_var(--color-surface),_var(--color-background),_var(--color-background))]">
      <div className="max-w-7xl mx-auto space-y-8">

        {/* Header */}
        <header className="text-center space-y-4 relative">
          <div className="absolute top-0 right-0 flex items-center justify-end z-10 w-full md:w-auto mt-4 md:mt-0 right-4">
            {user ? (
               <div className="flex items-center gap-3 bg-slate-800/80 backdrop-blur border border-slate-700 px-3 py-1.5 rounded-xl shadow-lg">
                 <span className="text-xs text-slate-300 hidden sm:inline">{user.email}</span>
                 <button onClick={handleLogout} className="text-slate-400 hover:text-white text-xs px-2 py-1 rounded transition bg-slate-700/50 hover:bg-red-500/80">Logout</button>
               </div>
            ) : (
               <button onClick={handleLogin} className="bg-blue-600 hover:bg-blue-500 border border-blue-500 text-white text-xs font-semibold px-4 py-2 rounded-xl transition-all shadow-lg hover:shadow-blue-500/25 flex items-center gap-2">
                 Log in
               </button>
            )}
          </div>
          <div className="inline-flex items-center justify-center p-3 glass-panel mb-4 shadow-blue-500/20 md:mt-4">
            <BarChart2 className="w-8 h-8 text-[var(--color-primary)]" />
          </div>
          <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-400">
            DIANA
          </h1>
          <p className="text-lg text-[var(--color-text-muted)] max-w-2xl mx-auto">
            Intelligent ETL & Analytics Platform — Upload, Transform, Analyze, and Generate AI Reports
          </p>
        </header>

        <main className="grid grid-cols-1 lg:grid-cols-12 gap-8">

          {/* Controls Column */}
          <div className="lg:col-span-4 space-y-6">
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
                  <label className="text-sm font-medium text-slate-300">Dataset File</label>
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
                      accept=".csv,.xlsx,.xls,.json"
                      className="hidden"
                    />
                    {file ? (
                      <div className="space-y-3">
                        <div className="mx-auto w-12 h-12 rounded-full bg-blue-500/20 flex items-center justify-center">
                          <FileType className="w-6 h-6 text-[var(--color-primary)]" />
                        </div>
                        <div>
                          <p className="text-sm font-semibold text-white">{file.name}</p>
                          <p className="text-xs text-slate-400 mt-1">{(file.size / 1024 / 1024).toFixed(2)} MB</p>
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        <div className="mx-auto w-12 h-12 rounded-full bg-slate-800 flex items-center justify-center group-hover:scale-110 transition-transform">
                          <Upload className="w-6 h-6 text-slate-400 group-hover:text-white" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-slate-300">Click to upload or drag and drop</p>
                          <p className="text-xs text-slate-500 mt-1">CSV, Excel (.xlsx), or JSON files</p>
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
                        <Loader2 className="w-5 h-5 animate-spin" />
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
          <div className="lg:col-span-8 space-y-6">

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
                  Upload a CSV, Excel, or JSON file and run the ETL pipeline to see results here.
                </p>
              </div>
            )}

            {/* Loading State */}
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
                </div>
              </div>
            )}

            {/* Results Display with Tabs */}
            {result && !isLoading && (
              <div className="animate-in fade-in slide-in-from-right-8 duration-500">

                {/* Warnings */}
                {result.warnings && result.warnings.length > 0 && (
                  <div className="mb-4 space-y-2">
                    {result.warnings.map((warn: string, i: number) => (
                      <Alert key={i} type="info" message={warn} />
                    ))}
                  </div>
                )}

                {/* Tab Navigation */}
                <div className="flex gap-1 overflow-x-auto pb-1 mb-1 scrollbar-thin">
                  {TABS.map(tab => {
                    const Icon = tab.icon;
                    return (
                      <button
                        key={tab.id}
                        onClick={() => handleTabClick(tab.id)}
                        className={`flex items-center gap-1.5 px-3 py-2.5 rounded-t-xl text-xs font-medium whitespace-nowrap transition-all ${
                          activeTab === tab.id
                            ? 'bg-slate-800/80 text-white border border-slate-700/50 border-b-transparent -mb-px z-10'
                            : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/30'
                        }`}
                      >
                        <Icon className="w-3.5 h-3.5" />
                        {tab.label}
                      </button>
                    );
                  })}
                </div>

                {/* Tab Content */}
                <div className="glass-panel overflow-hidden min-h-[400px]">
                  {activeTab === 'preview' && (
                    previewData ? (
                      <div className="flex flex-col">
                        <div className="px-6 py-4 border-b border-slate-700/50 bg-slate-900/50 flex justify-between items-center">
                          <h3 className="text-sm font-medium text-white">Data Preview (Top 100 Rows)</h3>
                          <span className="text-xs text-slate-400">{previewData.headers.length} columns</span>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm text-left">
                            <thead className="text-xs text-slate-400 bg-slate-800/50 uppercase border-b border-slate-700">
                              <tr>
                                {previewData.headers.map((header, i) => (
                                  <th key={i} className="px-6 py-3 font-medium whitespace-nowrap">{header}</th>
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

                        {/* Render inline artifacts if present */}
                        {result?.artifacts && Object.keys(result.artifacts).length > 0 && (
                          <div className="p-6 border-t border-slate-700/50 space-y-4">
                            <h3 className="text-sm font-medium text-white mb-4">Generated Visualizations</h3>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              {Object.entries(result.artifacts).map(([key, url]) => {
                                // Only show HTML charts inline
                                if (key.endsWith('_html') && typeof url === 'string') {
                                  return (
                                    <div key={key} className="bg-slate-800/40 rounded-xl border border-slate-700/50 overflow-hidden">
                                      <div className="p-3 border-b border-slate-700/40 flex items-center justify-between bg-slate-800">
                                        <span className="text-xs font-mono text-blue-400 capitalize">{key.replace('_html', '').replace('_', ' ')}</span>
                                        <a href={`${API}${url}`} target="_blank" rel="noreferrer" className="text-xs text-blue-400 hover:text-blue-300">Open ↗</a>
                                      </div>
                                      <iframe
                                        src={`${API}${url}`}
                                        className="w-full h-80 border-0 bg-white"
                                        title={key}
                                      />
                                    </div>
                                  );
                                }
                                return null;
                              })}
                            </div>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="p-8 text-center">
                        <p className="text-slate-400">Loading data preview...</p>
                      </div>
                    )
                  )}
                  {activeTab === 'quality' && renderQualityTab()}
                  {activeTab === 'summary' && renderSummaryTab()}
                  {activeTab === 'viz' && renderVizTab()}
                  {activeTab === 'chat' && renderChatTab()}
                  {activeTab === 'report' && renderReportTab()}
                  {activeTab === 'log' && renderLogTab()}
                  {activeTab === 'history' && renderHistoryTab()}
                  {activeTab === 'export' && renderExportTab()}
                </div>
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}

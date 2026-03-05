"use client";

import {
    useState, useMemo, useRef, useCallback, useEffect,
} from "react";
import {
    useReactTable,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    getFacetedRowModel,
    getFacetedUniqueValues,
    flexRender,
    ColumnDef,
    SortingState,
    FilterFn,
    ColumnFiltersState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import * as XLSX from "xlsx";
import { ParsedRow } from "@/types";
import {
    Search, ArrowUpDown, SlidersHorizontal, AlertCircle,
    X, Download, Pencil, Check, ChevronDown, Trash2,
} from "lucide-react";

interface ResultsTableProps {
    data: ParsedRow[];
    onDupStats?: (skuDups: number, titleDups: number) => void;
}

// ─── Editable cell ─────────────────────────────────────────────────────────────
function EditableCell({ value, rowIndex, columnKey, onSave, highlight }: {
    value: string; rowIndex: number; columnKey: string;
    onSave: (ri: number, k: string, v: string) => void; highlight?: boolean;
}) {
    const [editing, setEditing] = useState(false);
    const [draft, setDraft] = useState(value);
    const commit = () => { setEditing(false); if (draft !== value) onSave(rowIndex, columnKey, draft); };
    if (editing) return (
        <span className="flex items-center gap-1">
            <input autoFocus value={draft}
                onChange={e => setDraft(e.target.value)} onBlur={commit}
                onKeyDown={e => { if (e.key === "Enter") commit(); if (e.key === "Escape") { setDraft(value); setEditing(false); } }}
                className="ring-1 ring-inset ring-emerald-400 rounded px-0.5 py-0 h-5 text-xs font-mono w-full min-w-[100px] focus:outline-none bg-emerald-50" />
            <button onClick={commit} className="text-emerald-600 flex-shrink-0"><Check size={13} /></button>
        </span>
    );
    return (
        <span
            className={`group inline-flex items-center gap-1 cursor-pointer rounded px-1 -mx-1 hover:bg-gray-100/70 w-full overflow-hidden min-h-[20px] ${highlight ? "text-emerald-700 font-semibold" : ""}`}
            onClick={() => setEditing(true)} title={value || "Click to edit"}
        >
            <span className="font-mono text-xs truncate">{value || <span className="text-gray-300">—</span>}</span>
            <Pencil size={10} className="opacity-0 group-hover:opacity-40 flex-shrink-0" />
        </span>
    );
}

// ─── Column filter — fixed-position dropdown so overflow:hidden never clips it ──
function ColumnFilter({ columnId, table }: {
    columnId: string; table: ReturnType<typeof useReactTable<ParsedRow>>;
}) {
    const [open, setOpen] = useState(false);
    const [pos, setPos] = useState({ top: 0, left: 0 });
    const btnRef = useRef<HTMLButtonElement>(null);
    const dropRef = useRef<HTMLDivElement>(null);
    const column = table.getColumn(columnId);

    const uniqueVals = useMemo(() => {
        if (!column) return [];
        const map = column.getFacetedUniqueValues();
        return Array.from(map.keys()).map(v => String(v ?? "")).filter(Boolean).sort();
    }, [column]);

    const current: string[] = column ? ((column.getFilterValue() as string[]) ?? []) : [];
    const isFiltered = current.length > 0;

    const openDropdown = (e: React.MouseEvent) => {
        e.stopPropagation();
        if (!column) return;
        if (btnRef.current) {
            const r = btnRef.current.getBoundingClientRect();
            setPos({ top: r.bottom + 4, left: r.left });
        }
        setOpen(o => !o);
    };

    const toggle = (val: string) => {
        if (!column) return;
        const next = current.includes(val) ? current.filter(v => v !== val) : [...current, val];
        column.setFilterValue(next.length ? next : undefined);
    };

    useEffect(() => {
        if (!open || !column) return;
        const handler = (e: MouseEvent) => {
            if (!dropRef.current?.contains(e.target as Node) && !btnRef.current?.contains(e.target as Node))
                setOpen(false);
        };
        const handleScroll = () => setOpen(false);
        document.addEventListener("mousedown", handler);
        window.addEventListener("scroll", handleScroll, true);
        return () => {
            document.removeEventListener("mousedown", handler);
            window.removeEventListener("scroll", handleScroll, true);
        };
    }, [open, column]);

    if (!column) return null;

    return (
        <>
            <button ref={btnRef} onClick={openDropdown}
                className={`relative ml-0.5 flex-shrink-0 rounded p-0.5 transition-colors ${isFiltered ? "text-emerald-600 bg-emerald-100" : "text-gray-400 hover:text-gray-600 hover:bg-gray-100"}`}
                title="Filter column"
            >
                <ChevronDown size={11} />
                {isFiltered && <span className="absolute -top-0.5 -right-0.5 w-1.5 h-1.5 bg-emerald-500 rounded-full" />}
            </button>

            {open && (
                <div ref={dropRef}
                    className="fixed z-[9999] bg-white border border-gray-200 rounded-lg shadow-2xl min-w-[180px] max-w-[260px]"
                    style={{ top: pos.top, left: pos.left, maxHeight: 280, overflowY: "auto" }}
                >
                    <div className="flex items-center justify-between px-3 py-2 border-b sticky top-0 bg-white z-10">
                        <span className="text-xs font-semibold text-gray-600">Filter</span>
                        <div className="flex gap-2">
                            <button onClick={() => column.setFilterValue(undefined)} className="text-xs text-gray-400 hover:text-red-500">Clear</button>
                            <button onClick={() => setOpen(false)} className="text-gray-400 hover:text-gray-600"><X size={12} /></button>
                        </div>
                    </div>
                    <div className="p-1">
                        {uniqueVals.length === 0 && <p className="text-xs text-gray-400 px-2 py-2">No values</p>}
                        {uniqueVals.map(val => (
                            <label key={val} className="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-gray-50 cursor-pointer text-xs text-gray-700 select-none">
                                <input type="checkbox" checked={current.includes(val)} onChange={() => toggle(val)}
                                    className="rounded border-gray-300 text-emerald-600 h-3 w-3 flex-shrink-0" />
                                <span className="truncate font-mono" title={val}>{val}</span>
                            </label>
                        ))}
                    </div>
                </div>
            )}
        </>
    );
}

// ─── Resize handle ─────────────────────────────────────────────────────────────
function ResizeHandle({ onMouseDown }: { onMouseDown: (e: React.MouseEvent) => void }) {
    return (
        <div onMouseDown={onMouseDown}
            className="absolute right-0 top-0 bottom-0 w-3 cursor-col-resize flex items-center justify-center group z-10"
            onClick={e => e.stopPropagation()}>
            <div className="w-px h-4 bg-gray-300 group-hover:bg-emerald-400 group-hover:w-0.5 transition-all rounded-full" />
        </div>
    );
}

// ─── Multi-select filter fn ────────────────────────────────────────────────────
const multiValueFilter: FilterFn<ParsedRow> = (row, colId, filterValue: string[]) => {
    if (!filterValue?.length) return true;
    const cellVal = String(row.getValue(colId) ?? "");
    return filterValue.includes(cellVal);
};
multiValueFilter.autoRemove = (val: unknown) => !val || !(val as string[]).length;

// ─── Default column widths ─────────────────────────────────────────────────────
const DEFAULT_WIDTHS: Record<string, number> = {
    "Product Name": 380,
    "Product SKU": 240,
    "Product Web SKU": 220,
    "Product New SKU": 310,
    "SKU Duplicate": 120,
    "Title Duplicate": 120,
};
const COL_IDS = ["Product Name", "Product SKU", "Product Web SKU", "Product New SKU", "SKU Duplicate", "Title Duplicate"];

// ─── Main component ────────────────────────────────────────────────────────────
export function ResultsTable({ data, onDupStats }: ResultsTableProps) {
    const [searchInput, setSearchInput] = useState("");
    const [globalFilter, setGlobalFilter] = useState("");
    const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
    const [sorting, setSorting] = useState<SortingState>([]);
    const [showOnlyDuplicates, setShowOnlyDuplicates] = useState(false);
    const [rowEdits, setRowEdits] = useState<Record<number, Partial<ParsedRow>>>({});
    const [deletedRows, setDeletedRows] = useState<Set<number>>(new Set());
    const [activeDupKey, setActiveDupKey] = useState<{ field: "sku" | "title"; value: string } | null>(null);
    const [colWidths, setColWidths] = useState<Record<string, number>>(DEFAULT_WIDTHS);
    const colWidthsRef = useRef<Record<string, number>>(DEFAULT_WIDTHS);
    const colGroupRef = useRef<(HTMLTableColElement | null)[]>([]);

    const scrollRef = useRef<HTMLDivElement>(null);
    const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const resizeDrag = useRef<{ id: string; startX: number; startW: number; idx: number } | null>(null);

    // Debounced search
    const handleSearchChange = (val: string) => {
        setSearchInput(val);
        if (debounceRef.current) clearTimeout(debounceRef.current);
        debounceRef.current = setTimeout(() => setGlobalFilter(val), 250);
    };

    // DOM-only resize during drag; single setState on mouseUp
    const startResize = useCallback((e: React.MouseEvent, colId: string) => {
        e.preventDefault();
        const idx = COL_IDS.indexOf(colId);
        resizeDrag.current = { id: colId, startX: e.clientX, startW: colWidthsRef.current[colId] ?? 160, idx };
        document.body.style.cursor = "col-resize";
        document.body.style.userSelect = "none";
        const onMove = (ev: MouseEvent) => {
            if (!resizeDrag.current) return;
            const newW = Math.max(60, resizeDrag.current.startW + (ev.clientX - resizeDrag.current.startX));
            colWidthsRef.current = { ...colWidthsRef.current, [resizeDrag.current.id]: newW };
            const col = colGroupRef.current[resizeDrag.current.idx];
            if (col) col.style.width = `${newW}px`;
        };
        const onUp = () => {
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
            setColWidths({ ...colWidthsRef.current });
            resizeDrag.current = null;
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
        };
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup", onUp);
    }, []);

    // O(1) index map — not needed; _origIndex embedded in each row
    // Merge edits, exclude deleted rows, embed original index
    const mergedData = useMemo(() =>
        data
            .map((r, i) => ({ ...(rowEdits[i] ? { ...r, ...rowEdits[i] } : r), _origIndex: i }))
            .filter(r => !deletedRows.has(r._origIndex as number)),
        [data, rowEdits, deletedRows]
    );

    // Live duplicate recomputation — updates instantly when SKUs are edited or rows deleted
    const liveData = useMemo(() => {
        const skuCounts: Record<string, number> = {};
        const titleCounts: Record<string, number> = {};
        mergedData.forEach(r => {
            const sku = String(r["Product New SKU"] ?? "").trim().toUpperCase();
            if (sku) skuCounts[sku] = (skuCounts[sku] ?? 0) + 1;
            const title = String(r["Product Name"] ?? "").trim().toLowerCase();
            if (title) titleCounts[title] = (titleCounts[title] ?? 0) + 1;
        });
        return mergedData.map(r => ({
            ...r,
            "SKU Duplicate": (() => {
                const sku = String(r["Product New SKU"] ?? "").trim().toUpperCase();
                return sku && (skuCounts[sku] ?? 0) > 1 ? "DUPLICATED" : "";
            })(),
            "Title Duplicate": (() => {
                const title = String(r["Product Name"] ?? "").trim().toLowerCase();
                return title && (titleCounts[title] ?? 0) > 1 ? "DUPLICATED" : "";
            })(),
        }));
    }, [mergedData]);

    const handleCellSave = useCallback((ri: number, k: string, v: string) => {
        setRowEdits(prev => ({ ...prev, [ri]: { ...prev[ri], [k]: v } }));
    }, []);

    const handleDelete = useCallback((origIdx: number) => {
        setDeletedRows(prev => new Set([...prev, origIdx]));
    }, []);

    // Dup groups
    const dupGroups = useMemo(() => {
        const skuMap: Record<string, number[]> = {};
        const titleMap: Record<string, number[]> = {};
        liveData.forEach((r, i) => {
            [String(r["Product New SKU"] ?? "").trim().toUpperCase(),
            String(r["Product SKU"] ?? "").trim().toUpperCase(),
            String(r["Product Web SKU"] ?? "").trim().toUpperCase()]
                .filter(Boolean).forEach(s => { if (!skuMap[s]) skuMap[s] = []; skuMap[s].push(i); });
            const n = String(r["Product Name"] ?? "").trim().toLowerCase();
            if (n) { if (!titleMap[n]) titleMap[n] = []; titleMap[n].push(i); }
        });
        return { sku: skuMap, title: titleMap };
    }, [liveData]);

    const activeDupIndices = useMemo<Set<number>>(() => {
        if (!activeDupKey) return new Set();
        return new Set((activeDupKey.field === "sku" ? dupGroups.sku : dupGroups.title)[activeDupKey.value] ?? []);
    }, [activeDupKey, dupGroups]);

    const baseData = useMemo(() => {
        if (activeDupKey) return liveData.filter((_, i) => activeDupIndices.has(i));
        if (showOnlyDuplicates) return liveData.filter(r => r["SKU Duplicate"] === "DUPLICATED" || r["Title Duplicate"] === "DUPLICATED");
        return liveData;
    }, [liveData, showOnlyDuplicates, activeDupKey, activeDupIndices]);

    const columns = useMemo<ColumnDef<ParsedRow>[]>(() => [
        {
            id: "Product Name", accessorKey: "Product Name", header: "Product Name", filterFn: multiValueFilter,
            cell: ({ row, getValue }) => {
                const val = getValue() as string;
                const idx = row.original._origIndex as number;
                return <EditableCell value={val} rowIndex={idx} columnKey="Product Name" onSave={handleCellSave} />;
            },
        },
        {
            id: "Product SKU", accessorKey: "Product SKU", header: "Product SKU", filterFn: multiValueFilter,
            cell: ({ row, getValue }) => {
                const val = getValue() as string;
                const idx = row.original._origIndex as number;
                return <EditableCell value={val} rowIndex={idx} columnKey="Product SKU" onSave={handleCellSave} />;
            },
        },
        {
            id: "Product Web SKU", accessorKey: "Product Web SKU", filterFn: multiValueFilter,
            header: () => (
                <span className="flex items-center gap-1">
                    Product Web SKU
                    <span className="text-[9px] font-bold bg-emerald-100 text-emerald-700 px-1 rounded uppercase">src</span>
                </span>
            ),
            cell: ({ getValue }) => {
                const val = String(getValue() ?? "").trim();
                return val ? (
                    <span className="font-mono text-xs text-gray-600 truncate block select-all" title={val}>{val}</span>
                ) : (
                    <span className="inline-flex items-center gap-1 text-orange-400 text-xs font-medium">
                        <span className="w-1.5 h-1.5 rounded-full bg-orange-400 flex-shrink-0 inline-block" />missing
                    </span>
                );
            },
        },
        {
            id: "Product New SKU", accessorKey: "Product New SKU", header: "Product New SKU", filterFn: multiValueFilter,
            cell: ({ row, getValue }) => {
                const val = getValue() as string;
                const idx = row.original._origIndex as number;
                const isError = val && (val.includes("ERROR") || val === "NOT UNDERSTANDABLE TITLE");
                const isEdited = !!rowEdits[idx]?.["Product New SKU"];
                return isError ? (
                    <span className="text-orange-600 text-xs inline-flex items-center gap-1 bg-orange-50 px-1.5 py-0.5 rounded">
                        <AlertCircle size={12} />{val}
                    </span>
                ) : (
                    <EditableCell value={val} rowIndex={idx} columnKey="Product New SKU" onSave={handleCellSave} highlight={isEdited} />
                );
            },
        },
        {
            id: "SKU Duplicate", accessorKey: "SKU Duplicate", header: "SKU Dup", filterFn: multiValueFilter,
            cell: ({ row, getValue }) => {
                const val = getValue() as string;
                if (val !== "DUPLICATED") return <span className="text-gray-300">—</span>;
                const skuVal = String(row.original["Product New SKU"] ?? "").trim().toUpperCase() || String(row.original["Product SKU"] ?? "").trim().toUpperCase();
                return (
                    <button onClick={() => setActiveDupKey(p => p?.field === "sku" && p.value === skuVal ? null : { field: "sku", value: skuVal })}
                        className="inline-flex items-center rounded-full bg-red-50 px-2 py-0.5 text-xs font-medium text-red-700 ring-1 ring-inset ring-red-600/10 hover:bg-red-100 whitespace-nowrap">
                        Dup ↗
                    </button>
                );
            },
        },
        {
            id: "Title Duplicate", accessorKey: "Title Duplicate", header: "Title Dup", filterFn: multiValueFilter,
            cell: ({ row, getValue }) => {
                const val = getValue() as string;
                if (val !== "DUPLICATED") return <span className="text-gray-300">—</span>;
                const titleVal = String(row.original["Product Name"] ?? "").trim().toLowerCase();
                return (
                    <button onClick={() => setActiveDupKey(p => p?.field === "title" && p.value === titleVal ? null : { field: "title", value: titleVal })}
                        className="inline-flex items-center rounded-full bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-700 ring-1 ring-inset ring-amber-600/20 hover:bg-amber-100 whitespace-nowrap">
                        Dup ↗
                    </button>
                );
            },
        },
    ], [rowEdits, handleCellSave]);

    const table = useReactTable({
        data: baseData, columns,
        state: { sorting, globalFilter, columnFilters },
        onSortingChange: setSorting,
        onGlobalFilterChange: setGlobalFilter,
        onColumnFiltersChange: setColumnFilters,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFacetedRowModel: getFacetedRowModel(),
        getFacetedUniqueValues: getFacetedUniqueValues(),
    });

    const tableRows = table.getRowModel().rows;
    const totalFiltered = table.getFilteredRowModel().rows.length;
    const editCount = Object.keys(rowEdits).length;
    const hasColFilter = columnFilters.length > 0;

    const virtualizer = useVirtualizer({
        count: tableRows.length,
        getScrollElement: () => scrollRef.current,
        estimateSize: () => 41,
        overscan: 15,
    });
    const virtualItems = virtualizer.getVirtualItems();
    const totalVirtualHeight = virtualizer.getTotalSize();

    const downloadEdited = () => {
        const wb = XLSX.utils.book_new();
        const rows = liveData.map(r => ({
            "Product Name": r["Product Name"] ?? "",
            "Product SKU": r["Product SKU"] ?? "",
            "Product Web SKU": r["Product Web SKU"] ?? "",
            "Product New SKU": r["Product New SKU"] ?? "",
            "SKU Duplicate": r["SKU Duplicate"] ?? "",
            "Title Duplicate": r["Title Duplicate"] ?? "",
        }));
        XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), "Processed");
        XLSX.writeFile(wb, "products_sku_edited.xlsx");
    };

    const totalW = COL_IDS.reduce((s, id) => s + (colWidths[id] ?? 160), 0) + 44; // +44 for delete col

    return (
        <div className="bg-white border rounded-xl shadow-sm overflow-hidden flex flex-col">
            {/* Toolbar */}
            <div className="p-4 border-b flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center bg-gray-50/50">
                <div className="relative w-full sm:max-w-xs">
                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                        <Search size={15} className="text-gray-400" />
                    </div>
                    <input type="text" value={searchInput} onChange={e => handleSearchChange(e.target.value)}
                        className="block w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg bg-white placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-emerald-500 text-sm"
                        placeholder="Search all columns..." />
                </div>
                <div className="flex flex-wrap items-center gap-3 w-full sm:w-auto">
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 cursor-pointer select-none bg-white border border-gray-200 px-3 py-2 rounded-lg hover:bg-gray-50">
                        <input type="checkbox" checked={showOnlyDuplicates} onChange={e => { setShowOnlyDuplicates(e.target.checked); setActiveDupKey(null); }}
                            className="rounded border-gray-300 text-emerald-600 h-4 w-4" />
                        <SlidersHorizontal size={13} className="text-gray-500" />Duplicates only
                    </label>
                    {hasColFilter && (
                        <button onClick={() => setColumnFilters([])} className="flex items-center gap-1 text-xs font-medium text-red-600 bg-red-50 border border-red-200 px-3 py-2 rounded-lg hover:bg-red-100">
                            <X size={12} /> Clear filters
                        </button>
                    )}
                    {deletedRows.size > 0 && (
                        <button onClick={() => setDeletedRows(new Set())} className="flex items-center gap-1 text-xs font-medium text-gray-500 bg-white border border-gray-200 px-3 py-2 rounded-lg hover:bg-gray-50">
                            <Trash2 size={12} /> Restore {deletedRows.size} deleted
                        </button>
                    )}
                    <div className="text-sm text-gray-500 bg-white border border-gray-200 px-3 py-2 rounded-lg whitespace-nowrap">
                        <span className="font-semibold text-gray-900">{totalFiltered}</span> rows
                        {deletedRows.size > 0 && <span className="ml-1 text-red-400">({deletedRows.size} deleted)</span>}
                    </div>
                    <button onClick={downloadEdited}
                        className="flex items-center gap-1.5 text-sm font-medium bg-emerald-600 hover:bg-emerald-700 text-white px-3 py-2 rounded-lg shadow-sm whitespace-nowrap">
                        <Download size={14} />
                        {editCount > 0 ? `Download (${editCount} edits)` : "Download XLSX"}
                    </button>
                </div>
            </div>

            {/* Dup banner */}
            {activeDupKey && (
                <div className="flex items-center justify-between px-4 py-2.5 bg-amber-50 border-b border-amber-200 text-amber-800 text-sm">
                    <span>
                        Showing <strong>{activeDupIndices.size}</strong> rows matching{" "}
                        {activeDupKey.field === "sku" ? "SKU" : "title"} duplicate:{" "}
                        <code className="bg-amber-100 px-1.5 py-0.5 rounded text-xs font-mono">{activeDupKey.value}</code>
                    </span>
                    <button onClick={() => setActiveDupKey(null)} className="ml-4 text-amber-600 hover:text-amber-800"><X size={16} /></button>
                </div>
            )}

            {/* Table */}
            <div ref={scrollRef} className="w-full overflow-auto" style={{ maxHeight: "calc(100vh - 280px)", overflowY: "scroll" }}>
                <table className="text-sm border-collapse" style={{ minWidth: totalW, width: "100%", tableLayout: "fixed" }}>
                    <colgroup>
                        {COL_IDS.map((id, i) => (
                            <col key={id}
                                ref={el => { colGroupRef.current[i] = el; }}
                                style={{ width: colWidths[id] ?? 160 }} />
                        ))}
                        <col style={{ width: 44 }} />{/* delete column */}
                    </colgroup>
                    <thead className="bg-gray-50 uppercase text-xs font-semibold text-gray-500 tracking-wider sticky top-0 z-20">
                        {table.getHeaderGroups().map(hg => (
                            <tr key={hg.id} className="divide-x divide-gray-200">
                                {hg.headers.map(header => (
                                    <th key={header.id}
                                        className="px-3 py-3 text-left select-none relative border-b border-gray-200 bg-gray-50"
                                    >
                                        {/* inner div clips text; th itself must NOT clip so dropdown escapes */}
                                        <div className="flex items-center gap-1 overflow-hidden pr-2">
                                            <button className="flex items-center gap-1 hover:text-gray-800 flex-1 min-w-0 overflow-hidden" onClick={header.column.getToggleSortingHandler()}>
                                                <span className="truncate">{flexRender(header.column.columnDef.header, header.getContext())}</span>
                                                <ArrowUpDown size={10} className={`flex-shrink-0 ${header.column.getIsSorted() ? "text-emerald-600 opacity-100" : "opacity-30"}`} />
                                            </button>
                                            <ColumnFilter columnId={header.id} table={table} />
                                        </div>
                                        <ResizeHandle onMouseDown={e => startResize(e, header.id)} />
                                    </th>
                                ))}
                                {/* Delete col header */}
                                <th className="border-b border-gray-200 bg-gray-50 w-11" />
                            </tr>
                        ))}
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-100">
                        {virtualItems.length > 0 && virtualItems[0].start > 0 && (
                            <tr><td colSpan={COL_IDS.length + 1} style={{ height: virtualItems[0].start }} /></tr>
                        )}
                        {tableRows.length > 0 ? virtualItems.map(vRow => {
                            const row = tableRows[vRow.index];
                            if (!row) return null;
                            const origIdx = row.original._origIndex as number;
                            const isEdited = !!rowEdits[origIdx];
                            const isInDup = activeDupKey ? activeDupIndices.has(origIdx) : false;
                            const isDup = row.original["SKU Duplicate"] === "DUPLICATED" || row.original["Title Duplicate"] === "DUPLICATED";
                            return (
                                <tr key={row.id} data-index={vRow.index} ref={virtualizer.measureElement}
                                    className={[
                                        "group hover:bg-emerald-50/30 transition-colors divide-x divide-gray-100",
                                        isEdited ? "bg-emerald-50/50 border-l-2 border-l-emerald-400" : "",
                                        isInDup ? "bg-amber-50/60" : "",
                                        !isInDup && isDup && !isEdited ? "bg-red-50/25" : "",
                                    ].filter(Boolean).join(" ")}
                                >
                                    {row.getVisibleCells().map(cell => (
                                        <td key={cell.id} className="px-3 py-2.5 text-gray-600 overflow-hidden">
                                            <div className="truncate">
                                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                            </div>
                                        </td>
                                    ))}
                                    {/* Delete button */}
                                    <td className="px-1 py-2.5 text-center w-11">
                                        <button
                                            onClick={() => handleDelete(origIdx)}
                                            className="opacity-0 group-hover:opacity-100 transition-opacity text-gray-300 hover:text-red-500 hover:bg-red-50 rounded p-1"
                                            title="Delete row"
                                        >
                                            <Trash2 size={13} />
                                        </button>
                                    </td>
                                </tr>
                            );
                        }) : (
                            <tr><td colSpan={COL_IDS.length + 1} className="px-4 py-16 text-center text-gray-400">No results found.</td></tr>
                        )}
                        {virtualItems.length > 0 && (() => {
                            const last = virtualItems[virtualItems.length - 1];
                            const rem = totalVirtualHeight - last.end;
                            return rem > 0 ? <tr><td colSpan={COL_IDS.length + 1} style={{ height: rem }} /></tr> : null;
                        })()}
                    </tbody>
                </table>
                {totalFiltered > 0 && (
                    <div className="py-3 flex items-center justify-center border-t border-gray-100">
                        <p className="text-xs text-gray-400">{totalFiltered} rows · drag column edges to resize · click cell to edit · hover row to delete</p>
                    </div>
                )}
            </div>
        </div>
    );
}

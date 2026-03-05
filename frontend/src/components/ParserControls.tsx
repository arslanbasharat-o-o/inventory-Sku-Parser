import { Loader2, Play, Trash2, Download } from "lucide-react";

interface ParserControlsProps {
    onGenerate: () => void;
    onClear: () => void;
    onDownload: () => void;
    isGenerating: boolean;
    hasData: boolean;
}

export function ParserControls({
    onGenerate,
    onClear,
    onDownload,
    isGenerating,
    hasData,
}: ParserControlsProps) {
    return (
        <div className="flex flex-col sm:flex-row items-center gap-3">
            <button
                onClick={onGenerate}
                disabled={isGenerating}
                className="w-full sm:w-auto flex items-center justify-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white px-5 py-2.5 rounded-lg font-medium transition-colors disabled:opacity-70 disabled:cursor-not-allowed shadow-sm shadow-emerald-600/20"
            >
                {isGenerating ? (
                    <>
                        <Loader2 size={18} className="animate-spin" />
                        Parsing...
                    </>
                ) : (
                    <>
                        <Play size={18} fill="currentColor" />
                        Generate SKUs
                    </>
                )}
            </button>

            {hasData && (
                <>
                    <button
                        onClick={onClear}
                        disabled={isGenerating}
                        className="w-full sm:w-auto flex items-center justify-center gap-2 bg-white hover:bg-red-50 text-red-600 border border-gray-200 hover:border-red-200 px-4 py-2.5 rounded-lg font-medium transition-colors shadow-sm disabled:opacity-50"
                    >
                        <Trash2 size={18} />
                        Clear Table
                    </button>

                    <button
                        onClick={onDownload}
                        disabled={isGenerating}
                        className="w-full sm:w-auto flex items-center justify-center gap-2 bg-white hover:bg-gray-50 text-gray-700 border border-gray-200 px-4 py-2.5 rounded-lg font-medium transition-colors shadow-sm disabled:opacity-50 ml-auto"
                    >
                        <Download size={18} />
                        Download Processed File
                    </button>
                </>
            )}
        </div>
    );
}

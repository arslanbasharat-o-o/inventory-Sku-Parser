import { UploadCloud, FileSpreadsheet } from "lucide-react";

interface FileUploaderProps {
    file: File | null;
    onFileSelect: (file: File) => void;
    isDragging: boolean;
    onDragEnter: (e: React.DragEvent) => void;
    onDragLeave: (e: React.DragEvent) => void;
    onDragOver: (e: React.DragEvent) => void;
    onDrop: (e: React.DragEvent) => void;
}

export function FileUploader({
    file,
    onFileSelect,
    isDragging,
    onDragEnter,
    onDragLeave,
    onDragOver,
    onDrop,
}: FileUploaderProps) {
    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files && e.target.files.length > 0) {
            onFileSelect(e.target.files[0]);
        }
    };

    return (
        <div className="w-full">
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Upload Inventory File</h2>
            <p className="text-sm text-gray-500 mb-4">
                Accepted formats: .xlsx, .csv (Max 20MB)
            </p>

            <div
                className={`relative w-full rounded-xl border-2 border-dashed p-8 transition-all duration-200 ease-in-out ${isDragging
                        ? "border-emerald-500 bg-emerald-50/50"
                        : file
                            ? "border-emerald-200 bg-emerald-50/30"
                            : "border-gray-200 hover:border-emerald-300 hover:bg-gray-50/50"
                    }`}
                onDragEnter={onDragEnter}
                onDragLeave={onDragLeave}
                onDragOver={onDragOver}
                onDrop={onDrop}
            >
                <input
                    type="file"
                    id="file-upload"
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                    accept=".xlsx,.csv"
                    onChange={handleFileChange}
                />
                <div className="flex flex-col items-center justify-center text-center gap-3">
                    {file ? (
                        <>
                            <div className="p-3 bg-emerald-100 rounded-full text-emerald-600">
                                <FileSpreadsheet size={28} />
                            </div>
                            <div className="space-y-1">
                                <p className="text-sm font-medium text-emerald-700">
                                    {file.name}
                                </p>
                                <p className="text-xs text-emerald-600/70">
                                    {(file.size / 1024 / 1024).toFixed(2)} MB
                                </p>
                            </div>
                        </>
                    ) : (
                        <>
                            <div className="p-3 bg-gray-100 rounded-full text-gray-500">
                                <UploadCloud size={28} />
                            </div>
                            <div>
                                <p className="text-sm font-medium text-emerald-600 hover:text-emerald-700">
                                    Click to upload <span className="text-gray-500 font-normal">or drag and drop</span>
                                </p>
                                <p className="text-xs text-gray-400 mt-1">Excel or CSV</p>
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

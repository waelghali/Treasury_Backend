import re

filename = r"c:\Grow\frontend\src\pages\EndUser\IssuedLGsPage.js"

with open(filename, "r", encoding="utf-8") as f:
    content = f.read()

# Chunk 1: allTabs
content = re.sub(
    r"""    const allTabs = \[
        \{ id: 'overview', label: 'Overview' \},
        \{ id: 'timeline', label: '📍 Timeline' \},
        \{ id: 'maintenance', label: '⚙️ Maintenance' \},
        \{ id: 'documents', label: '📄 Documents' \},
    \];""",
    """    const allTabs = [
        { id: 'overview', label: 'Overview' },
        { id: 'documents', label: '📄 Documents' },
        { id: 'lifecycle', label: '📍 Lifecycle History' },
    ];""",
    content
)

# Chunk 2: useEffect fetchMaintenanceActions
content = re.sub(
    r"""    useEffect\(\(\) => \{
        if \(activeTab === 'maintenance'\) fetchMaintenanceActions\(\);
    \}, \[activeTab\]\);""",
    """    useEffect(() => {
        if (lg && lg.id) fetchMaintenanceActions();
    }, [lg?.id]);""",
    content
)

# Chunk 3: Action Buttons Movement
# We must carefully replace the segment near `<div className="flex border-b border-slate-200 shrink-0">`
# The existing code has the `CHANGE_OWNERSHIP` button.
content = re.sub(
    r"""                        \{\!readOnly && availableActions\.some\(a => a\.type === 'CHANGE_OWNERSHIP'\) && \(
                            <button
                                onClick=\{\(\) => setShowChangeOwnerModal\(true\)\}
                                className="flex items-center gap-1\.5 px-3 py-1\.5 bg-slate-600 text-white text-xs font-bold rounded-lg hover:bg-slate-700 transition-all shadow-sm shrink-0"
                            >
                                <Users className="w-3\.5 h-3\.5" />
                                Change Owner
                            </button>
                        \)\}
                    </div>

                    \{\/\* Tabs \*\/\}
                    <div className="flex border-b border-slate-200 shrink-0">""",
    """                    </div>

                    {/* Global Actions Bar */}
                    {!readOnly && (
                        <div className="flex flex-wrap items-center gap-2 px-6 py-3 bg-white border-b border-slate-200 shrink-0">
                            {availableActions.map(action => {
                                const cfg = actionButtonConfig[action.type];
                                if (!cfg) return null;
                                const BtnIcon = cfg.icon;
                                return (
                                    <button key={action.type} onClick={() => { 
                                            if (action.type === 'CHANGE_OWNERSHIP') {
                                                setShowChangeOwnerModal(true);
                                            } else {
                                                setActionModal(action.type); setFormData({}); 
                                            }
                                        }}
                                        className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-white text-xs font-bold shadow-sm transition-all ${cfg.color.replace('py-2.5', 'py-1.5')}`}
                                    >
                                        <BtnIcon className="w-3.5 h-3.5" />
                                        {action.label || cfg.label}
                                    </button>
                                );
                            })}
                            <button onClick={() => { setBankInitiatedModal(true); setBankInitiatedFile(null); }}
                                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-white text-xs font-bold shadow-sm transition-all bg-violet-600 hover:bg-violet-700">
                                🏦 Bank-Initiated Change
                            </button>
                        </div>
                    )}

                    {/* Tabs */}
                    <div className="flex border-b border-slate-200 shrink-0">""",
    content
)

# Chunk 4: The Tabs replacement
# We search for the start: "{/* TAB: Timeline (Tracking + Activity Log) */}"
# And end right before "</div>\n\n                    {/* Footer */}"
# We will construct the new tabs.

tabs_regex = re.compile(
    r"                        \{\/\* TAB: Timeline \(Tracking \+ Activity Log\) \*\/\}.*?                </div>\n\n                    \{\/\* Footer \*\/\}\n",
    re.DOTALL
)

# We need to extract the parts we want to keep.
match = tabs_regex.search(content)
if match:
    old_tabs_block = match.group(0)
    
    # Extract AI Verification Result
    ai_result_match = re.search(r"(\{/\* AI Bank-Initiated Diff Result \*/\}.*?</div>\n                                \)\})", old_tabs_block, re.DOTALL)
    ai_result = ai_result_match.group(1) if ai_result_match else ""
    
    # Extract Maintenance History list
    maint_match = re.search(r"(\{loadingActions \? \(.*?</div>\n                                    \)\})", old_tabs_block, re.DOTALL)
    maint_list = maint_match.group(1) if maint_match else ""
    
    # Extract Activity Log (the timeline mapping)
    # Be careful there's two of them (Timeline & Audit). We just grab the first one under timeline.
    activity_match = re.search(r"(<h4 className=\"text-\[10px\] font-black text-slate-400 uppercase tracking-widest mb-4\">Activity Log</h4>.*?</div>\n                                    \)\})", old_tabs_block, re.DOTALL)
    activity_log = activity_match.group(1) if activity_match else ""
    
    new_tabs_block = f"""                        {{/* TAB: Lifecycle History */}}
                        {{activeTab === 'lifecycle' && (
                            <div className="space-y-6">
                                {{/* Top: Post-Issuance Graphical Tracker */}}
                                <div>
                                    <PostIssuanceTracker
                                        lgId={{lg.id}}
                                        readOnly={{readOnly}}
                                        onStatusChange={{() => {{ /* could refresh parent */ }}}}
                                    />
                                </div>
                                
                                {ai_result}

                                {{/* Middle/Bottom: Unified Feed Split */}}
                                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6 border-t border-slate-200 pt-6">
                                    {{/* Left: Comprehensive Maintenance Log */}}
                                    <div>
                                        <h4 className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-4">Maintenance & Actions Log</h4>
                                        {maint_list}
                                    </div>
                                    
                                    {{/* Right: Automated Application Lifecycle Events */}}
                                    <div>
                                        {activity_log.replace('Activity Log', 'Application Lifecycle')}
                                    </div>
                                </div>
                            </div>
                        )}}

                        {{/* TAB: Documents */}}
                        {{activeTab === 'documents' && (
                            <DocumentsTab lgId={{lg.id}} />
                        )}}
                    </div>

                    {{/* Footer */}}
"""
    content = content[:match.start()] + new_tabs_block + content[match.end():]
else:
    print("Could not find the tabs block to replace.")

with open(filename, "w", encoding="utf-8") as f:
    f.write(content)

print("Patching complete.")

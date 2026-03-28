import sys

file_path = "c:/Grow/frontend/src/pages/EndUser/IssuedLGsPage.js"
with open(file_path, "r", encoding="utf-8") as f:
    text = f.read()

split1 = text.find("                                {/* Top: Post-Issuance Graphical Tracker */}")
split2 = text.find("                                {/* Middle/Bottom: Unified Feed Split */}")
split3 = text.find("                                    {/* Left: Comprehensive Maintenance Log */}")
split4 = text.find("                                    {/* Right: Automated Application Lifecycle Events */}")
split5 = text.find("                                </div>\n                            </div>\n                        )}")

if -1 in [split1, split2, split3, split4, split5]:
    print("Could not find splits:", split1, split2, split3, split4, split5)
    sys.exit(1)

head = text[:split1]

top_block = text[split1:split2]
maintenance_block = text[split3:split4]
lifecycle_block = text[split4:split5]

tail = text[split5:] # from </div>\n</div>\n)} onwards

ai_split = top_block.find("                                {/* AI Bank-Initiated Diff Result */}")
track_block = top_block[:ai_split]
ai_block = top_block[ai_split:]

lifecycle_block = lifecycle_block.replace(
    "lifecycleTimeline.map(",
    "[...lifecycleTimeline].reverse().map("
)

# Strip out trailing newlines to keep it clean
track_block = track_block.rstrip() + "\n"
ai_block = ai_block.rstrip() + "\n"
lifecycle_block = lifecycle_block.rstrip() + "\n"
maintenance_block = maintenance_block.rstrip() + "\n"

new_active_tab = f"""{ai_block}
                                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6 border-t border-slate-200 pt-6">
                                    {/* Left: Tracker & Application Lifecycle */}
                                    <div className="space-y-6">
{track_block}
                                        <div className="border-t border-slate-200 pt-6">
{lifecycle_block}                                        </div>
                                    </div>

{maintenance_block}                                </div>
                            </div>
                        )}}
"""

# Replace the end </div></div>)} from tail so we don't duplicate it.
# Actually, tail starts with `split5` which is "                                </div>\n                            </div>\n                        )}"
# So if we use tail[len(split5_str):], we nicely append the rest of the file.
split5_str = "                                </div>\n                            </div>\n                        )}"
final_text = head + new_active_tab + tail[len(split5_str):]

with open(file_path, "w", encoding="utf-8") as f:
    f.write(final_text)

print("Layout successfully patched. Reversed timeline and swapped columns.")

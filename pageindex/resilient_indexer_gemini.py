import json
import os
import asyncio
import math
from pageindex import (
    generate_toc_init, 
    generate_toc_continue, 
    verify_toc,
    get_number_of_pages
)
from .utils import call_with_backoff

class ResilientPageIndexer:
    def __init__(self, pdf_path, opt, work_dir="./tmp"):
        self.pdf_path = pdf_path
        self.opt = opt  # Preservation of CLI settings
        self.doc_name = os.path.basename(pdf_path)
        self.state_path = os.path.join(work_dir, f"{self.doc_name}_state.json")
        os.makedirs(work_dir, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, 'r') as f:
                state = json.load(f)
                print(f"🔄 Checkpoint found. Resuming from step: {state['step']}")
                return state
        return {
            "step": "INITIALIZE",
            "completed_groups": [],
            "toc_segments": [],
            "total_groups": 0,
            "total_pages": 0
        }

    def _save_state(self):
        with open(self.state_path, 'w') as f:
            json.dump(self.state, f, indent=4)

    async def run(self):
        # Step 0: Get Page Count (Local call, no backoff needed)
        if self.state["total_pages"] == 0:
            self.state["total_pages"] = get_number_of_pages(self.pdf_path)
            # Calculate groups exactly how the library does (90 pages / 10 max = 9 groups)
            self.state["total_groups"] = math.ceil(self.state["total_pages"] / self.opt.max_page_num_each_node)
            self._save_state()

        # Step 1: Initial TOC Analysis (LLM Reasoning)
        if self.state["step"] == "INITIALIZE":
            print(f"🚀 Initializing Index for {self.doc_name} ({self.state['total_pages']} pages)...")
            # We pass the opt object to ensure TOC check page limits are respected
            await call_with_backoff(generate_toc_init, self.pdf_path, self.opt)
            self.state["step"] = "EXTRACT_NODES"
            self._save_state()

#        # Step 2: Resumable Node Extraction (update suggestion: didn't work)
#        if self.state["step"] == "EXTRACT_NODES":
#            total = self.state["total_groups"]
#            for i in range(1, total + 1):
#                group_key = str(i) # Use string for state keys
#                if group_key in self.state["completed_groups"]:
#                    print(f"⏩ Group {i}/{total} already completed. Skipping.")
#                    continue
#                
#                print(f"🧠 Processing Group {i}/{total}...")
#                
#                # RELEVANT FIX: Pass i as an int (standard), 
#                # but ensure self.pdf_path is definitely a string
#                segment = await call_with_backoff(
#                    generate_toc_continue, 
#                    str(self.pdf_path), # Force path to string
#                    i,                  # Group index as int
#                    self.opt            # Config object
#                )
#                
#                self.state["toc_segments"].append(segment)
#                self.state["completed_groups"].append(group_key)
#                self._save_state()
                
        # Step 2: Resumable Node Extraction (original)
        if self.state["step"] == "EXTRACT_NODES":
            total = self.state["total_groups"]
            for i in range(1, total + 1):
                if str(i) in self.state["completed_groups"]:
                    print(f"⏩ Group {i}/{total} already completed. Skipping.")
                    continue
                
                print(f"🧠 Processing Group {i}/{total}...")
                # We pass the opt here so summaries/node-id flags are respected
                segment = await call_with_backoff(generate_toc_continue, self.pdf_path, str(i), self.opt)
                
                self.state["toc_segments"].append(segment)
                self.state["completed_groups"].append(str(i))
                self._save_state()
                
                # Small safety pause to prevent hitting TPM limits prematurely
                await asyncio.sleep(0.5)

            self.state["step"] = "VERIFY"
            self._save_state()

        # Step 3: Final Verification and Tree Assembly
        if self.state["step"] == "VERIFY":
            print("🧐 Verifying structure and building final tree...")
            # verify_toc uses the segments to build the final JSON mapping
            final_structure = await call_with_backoff(verify_toc, self.state["toc_segments"], self.opt)
            
            # Success!
            print(f"✅ Full tree generated for {self.doc_name}")
            # We keep the state file for auditing, but you could delete it here
            return final_structure

        # If already completed in a previous run
        if self.state["step"] == "COMPLETED":
             # This would require saving the actual final JSON in the state, 
             # usually better to just re-verify from segments.
             return await call_with_backoff(verify_toc, self.state["toc_segments"], self.opt)
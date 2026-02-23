import os
import json
import argparse
import asyncio
from pageindex import config, md_to_tree
from pageindex.utils import ConfigLoader
from pageindex.resilient_indexer import ResilientPageIndexer

async def process_pdf_robustly(args):
    # Configure options using the library's internal config tool
    opt = config(
        model=args.model,
        toc_check_page_num=args.toc_check_pages,
        max_page_num_each_node=args.max_pages_per_node,
        max_token_num_each_node=args.max_tokens_per_node,
        if_add_node_id=args.if_add_node_id,
        if_add_node_summary=args.if_add_node_summary,
        if_add_doc_description=args.if_add_doc_description,
        if_add_node_text=args.if_add_node_text
    )

    indexer = ResilientPageIndexer(args.pdf_path, opt=opt)
    
    try:
        toc_with_page_number = await indexer.run()
        
        # Save results
        pdf_name = os.path.splitext(os.path.basename(args.pdf_path))[0]    
        output_dir = './results'
        output_file = f'{output_dir}/{pdf_name}_structure.json'
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(toc_with_page_number, f, indent=2)
        
        print(f'🎉 Success! Result saved to: {output_file}')
        
    except Exception as e:
        print(f"\n❌ Execution Interrupted: {e}")
        print(f"💡 To resume, simply run this command again. State is saved in ./tmp")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Robust PageIndex Processor')
    # All original arguments preserved
    parser.add_argument('--pdf_path', type=str, help='Path to PDF')
    parser.add_argument('--md_path', type=str, help='Path to Markdown')
    parser.add_argument('--model', type=str, default='gpt-4o-2024-11-20')
    parser.add_argument('--toc-check-pages', type=int, default=20)
    parser.add_argument('--max-pages-per-node', type=int, default=10)
    parser.add_argument('--max-tokens-per-node', type=int, default=20000)
    parser.add_argument('--if-add-node-id', type=str, default='yes')
    parser.add_argument('--if-add-node-summary', type=str, default='yes')
    parser.add_argument('--if-add-doc-description', type=str, default='no')
    parser.add_argument('--if-add-node-text', type=str, default='no')
    
    args = parser.parse_args()

    if args.pdf_path:
        asyncio.run(process_pdf_robustly(args))
    elif args.md_path:
        # Markdown is typically one single call, but we can still protect it
        print("Markdown mode is not yet checkpointed but is protected by backoff.")
        
        # Validate Markdown file
        if not args.md_path.lower().endswith(('.md', '.markdown')):
            raise ValueError("Markdown file must have .md or .markdown extension")
        if not os.path.isfile(args.md_path):
            raise ValueError(f"Markdown file not found: {args.md_path}")
            
        print(f'Processing markdown file: {args.md_path}...')
        
        # 1. Use ConfigLoader to get consistent defaults matching PDF behavior
        from pageindex.utils import ConfigLoader
        config_loader = ConfigLoader()
        
        user_opt = {
            'model': args.model,
            'if_add_node_summary': args.if_add_node_summary,
            'if_add_doc_description': args.if_add_doc_description,
            'if_add_node_text': args.if_add_node_text,
            'if_add_node_id': args.if_add_node_id
        }
        
        # Load config with defaults from config.yaml
        opt = config_loader.load(user_opt)
        
        # 2. Execute with backoff protection
        # We wrap the md_to_tree call so it handles rate limits gracefully
        toc_with_page_number = asyncio.run(call_with_backoff(
            md_to_tree,
            md_path=args.md_path,
            if_thinning=args.if_thinning.lower() == 'yes',
            min_token_threshold=args.thinning_threshold,
            if_add_node_summary=opt.if_add_node_summary,
            summary_token_threshold=args.summary_token_threshold,
            model=opt.model,
            if_add_doc_description=opt.if_add_doc_description,
            if_add_node_text=opt.if_add_node_text,
            if_add_node_id=opt.if_add_node_id
        ))
        
        print('Parsing done, saving to file...')
        
        # 3. Save results
        md_name = os.path.splitext(os.path.basename(args.md_path))[0]    
        output_dir = './results'
        output_file = f'{output_dir}/{md_name}_structure.json'
        os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(toc_with_page_number, f, indent=2, ensure_ascii=False)
        
        print(f'Tree structure saved to: {output_file}')

import asyncio
import os
import logging
from playwright.async_api import async_playwright
from llama_parse import LlamaParse
import google.generativeai as genai
from dotenv import load_dotenv
import yaml
from pathlib import Path

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment file if present
load_dotenv()

# Load API keys from conf.yaml (fallback to environment variables)
CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"
try:
    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f) or {}
    secrets = cfg.get("secrets", {})
    LLAMA_PARSER_API_KEY = secrets.get("LLAMA_PARSER_API_KEY") or os.getenv("LLAMA_PARSER_API_KEY")
    GEMINI_API_KEY = secrets.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
except Exception as e:
    logger.warning(f"Could not load conf.yaml: {e}")
    LLAMA_PARSER_API_KEY = os.getenv("LLAMA_PARSER_API_KEY")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not LLAMA_PARSER_API_KEY or not GEMINI_API_KEY:
    logger.warning("LLAMA_PARSER_API_KEY or GEMINI_API_KEY not found in conf.yaml or environment variables.")

# Configure Gemini client
genai.configure(api_key=GEMINI_API_KEY)

# --- CONFIGURATION ---
CITY_URLS = {
    "islamabad": "https://bill.pitc.com.pk/iescobill",
    "lahore": "https://bill.pitc.com.pk/lescobill",
    "karachi": "https://bill.pitc.com.pk/hescobill",
    "gujranwala": "https://bill.pitc.com.pk/gepcobill",
    "faisalabad": "https://bill.pitc.com.pk/fescobill",
    "multan": "https://bill.pitc.com.pk/mepcobill",
    "peshawar": "https://bill.pitc.com.pk/pescobill",
    "hazara": "https://bill.pitc.com.pk/hazecobill",
    "hyderabad": "https://bill.pitc.com.pk/hescobill",
    "sukkur": "https://bill.pitc.com.pk/sepcobill",
    "quetta": "https://bill.pitc.com.pk/qescobill",
    "tribal": "https://bill.pitc.com.pk/tescoill",



}

async def capture_bill_pdf(city, ref_id, output_path="bill.pdf"):
    url = CITY_URLS.get(city.lower())
    
    async with async_playwright() as p:
        # Use a real browser context to avoid bot detection
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 1200}
        )
        page = await context.new_page()
        
        try:
            logger.info(f"🌐 Opening: {url}")
            await page.goto(url, wait_until="domcontentloaded")

            # 1. Fill the Reference Number
            logger.info(f"⌨️ Entering Reference ID: {ref_id}")
            await page.wait_for_selector("#searchTextBox", timeout=10000)
            await page.fill("#searchTextBox", ref_id)

            # 2. Click the Search button
            # We will listen for a popup BUT also continue if no popup appears
            logger.info("🖱️ Clicking Search Button...")
            
            # This creates a task that waits for a popup (if it happens)
            popup_task = asyncio.create_task(page.wait_for_event("popup", timeout=5000))
            
            await page.click("#btnSearch")

            # 3. Determine where the bill loaded (Same tab or New tab)
            try:
                # Try to get the popup if it appeared within 5 seconds
                bill_page = await popup_task
                logger.info("✅ Bill opened in a NEW tab.")
            except:
                # If no popup, the bill likely loaded in the SAME tab
                bill_page = page
                logger.info("✅ Bill loading in the SAME tab.")

            # 4. Wait for the bill content to actually appear
            # We look for "Reference No" or "Consumer Name" in the bill table
            logger.info("⏳ Waiting for bill data to render...")
            try:
                # Increased timeout to 20s because PITC databases are slow
                await bill_page.wait_for_selector("text=Reference", timeout=20000)
                # Small sleep to ensure CSS/Images are rendered for LlamaParse
                await asyncio.sleep(2) 
            except:
                logger.error("❌ Bill data not found on the page. Possible invalid ID or slow server.")
                await bill_page.screenshot(path="error_screen.png")
                return False

            # 5. Save the bill page as PDF
            await bill_page.emulate_media(media="screen")
            await bill_page.pdf(path=output_path, format="A4", print_background=True)
            
            logger.info(f"📄 PDF successfully saved to {output_path}")
            return True

        except Exception as e:
            logger.error(f"⚠️ Error: {e}")
            return False
        finally:
            await browser.close()

async def extract_text_from_pdf(file_path):
    logger.info(f"📁 Sending to LlamaParse: {file_path}")
    parser = LlamaParse(
        api_key=LLAMA_PARSER_API_KEY,
        result_type="text",
        verbose=True,
        language="en",
    )
    documents = await parser.aload_data(file_path)
    return "\n".join([doc.text for doc in documents])

async def beautify_with_gemini(raw_text):
    model = genai.GenerativeModel('gemini-2.5-flash')
    prompt = f"""
    Extract all details from this Pakistani Electricity Bill text and present them beautifully.
    Include:
    - Consumer Name & Address
    - Reference Number
    - Billing Month
    - Total Units Consumed
    - Amount Payable (Within Due Date)
    - Amount Payable (After Due Date)
    - Due Date
    - MONTH UNITS BILL PAYMENT
    - 	Off Peak	Peak	MDI -> Export(kWh)	 Import(kWh)	 Net(kWh)	
    
    Raw Data:
    {raw_text}
    """
    response = model.generate_content(prompt)
    return response.text

async def main():
    print("\n--- 💡 PITC Electricity Bill Extractor ---")
    # ref_number = input("Enter 14-digit Reference ID: ").strip()
    # city = input("Enter City (islamabad/lahore/karachi): ").strip().lower()
    ref_number = '0400029884879'
    city = 'karachi'
    temp_pdf = "output_bill.pdf"

    if await capture_bill_pdf(city, ref_number, temp_pdf):
        print("⏳ Parsing PDF with LlamaParse...")
        raw_text = await extract_text_from_pdf(temp_pdf)
        
        if len(raw_text) > 100:
            print("✨ Formatting with Gemini AI...")
            final_report = await beautify_with_gemini(raw_text)
            print("\n" + "="*60 + "\n" + final_report + "\n" + "="*60)
            # Save the final report to a text file
            with open("final_report_bill.txt", "w") as f:
                f.write(final_report)
        else:
            print("❌ Parsing failed: Could not find text in the generated PDF.")
        
        if os.path.exists(temp_pdf): os.remove(temp_pdf)
    else:
        print("❌ Could not retrieve bill. Check 'error_screen.png' if it exists.")

if __name__ == "__main__":
    asyncio.run(main())
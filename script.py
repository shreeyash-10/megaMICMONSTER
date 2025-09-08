import requests
import pandas as pd
import json
import time
from pathlib import Path
import base64
import concurrent.futures
import threading
from queue import Queue

# -----------------------------
# CONFIGURATION
# -----------------------------
excel_file = r"telugu_sentences (1).xlsx"

# API endpoint from your cURL
API_URL = "enter your API URL here"
API_KEY = "enter your API key here"

# Download directory
DOWNLOAD_DIR = Path("api_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Voice settings (from your cURL data)
VOICE_NAME = "te-IN-MohanNeural"  # Telugu voice
LOCALE = "te-IN"

# Parallel processing settings
MAX_WORKERS = 5  # Number of parallel API requests
REQUEST_DELAY = 0.5  # Delay between requests to avoid overwhelming API

# Progress tracking
progress_lock = threading.Lock()
global_stats = {"completed": 0, "failed": 0, "total": 0}


class MicMonsterAPI:
    def __init__(self, worker_id=None):
        self.session = requests.Session()
        self.worker_id = worker_id

        # Headers from your cURL request
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9,en-IN;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://micmonster.com',
            'Referer': 'https://micmonster.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Microsoft Edge";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

    def generate_audio(self, text, sentence_num):
        """Generate audio using MicMonster API."""
        try:
            if self.worker_id:
                print(f"Worker {self.worker_id}: Processing sentence {sentence_num}...")
            else:
                print(f"Processing sentence {sentence_num}...")

            # Format the request data exactly like your cURL
            data = {
                "content": f"<voice name='{VOICE_NAME}'>{text}</voice>",
                "locale": LOCALE,
                "ip": "40.77.167.10"  # From your cURL request
            }

            # Make the API request
            response = self.session.post(
                f"{API_URL}?code={API_KEY}",
                headers=self.headers,
                json=data,
                timeout=30
            )

            if response.status_code == 200:
                if response.text.strip():  # Check if response has content
                    try:
                        # Try to parse as JSON first
                        response_data = response.json()

                        # Handle JSON response with audio data
                        if 'audioContent' in response_data:
                            audio_data = base64.b64decode(response_data['audioContent'])
                            filename = f"sentence_{sentence_num:04d}.mp3"
                            filepath = DOWNLOAD_DIR / filename
                            with open(filepath, 'wb') as f:
                                f.write(audio_data)
                            return True

                    except json.JSONDecodeError:
                        # Response is not JSON - it's base64 encoded audio data
                        try:
                            # The response text is base64 encoded MP3
                            audio_data = base64.b64decode(response.text)
                            filename = f"sentence_{sentence_num:04d}.mp3"
                            filepath = DOWNLOAD_DIR / filename

                            with open(filepath, 'wb') as f:
                                f.write(audio_data)

                            return True

                        except Exception as decode_error:
                            # Try saving as raw bytes
                            filename = f"sentence_{sentence_num:04d}.mp3"
                            filepath = DOWNLOAD_DIR / filename
                            with open(filepath, 'wb') as f:
                                f.write(response.content)
                            return True
                else:
                    return False
            else:
                if self.worker_id:
                    print(f"Worker {self.worker_id}: API Error {response.status_code} for sentence {sentence_num}")
                return False

        except Exception as e:
            if self.worker_id:
                print(f"Worker {self.worker_id}: Error for sentence {sentence_num}: {e}")
            return False


def process_sentence_batch(batch_data):
    """Process a batch of sentences in parallel."""
    worker_id, sentences_batch = batch_data
    api_client = MicMonsterAPI(worker_id)

    batch_results = []

    for sentence_num, sentence in sentences_batch:
        try:
            success = api_client.generate_audio(sentence, sentence_num)
            batch_results.append((sentence_num, success, sentence))

            # Update global progress
            with progress_lock:
                if success:
                    global_stats["completed"] += 1
                else:
                    global_stats["failed"] += 1

                total_processed = global_stats["completed"] + global_stats["failed"]
                if total_processed % 25 == 0:  # Progress every 25 sentences
                    success_rate = (global_stats["completed"] / total_processed) * 100
                    print(f"\n📊 PROGRESS: {total_processed}/{global_stats['total']} ({success_rate:.1f}% success)")

            # Small delay to avoid overwhelming API
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"Worker {worker_id}: Exception processing sentence {sentence_num}: {e}")
            batch_results.append((sentence_num, False, sentence))

    return worker_id, batch_results


def main_parallel():
    """Main function with parallel processing."""
    print("Starting MicMonster API Parallel Processing")
    print("=" * 60)

    # Read Excel file
    try:
        df = pd.read_excel(excel_file)
        sentences = df.iloc[:, 0].dropna().tolist()
        global_stats["total"] = len(sentences)
        print(f"Loaded {len(sentences)} Telugu sentences")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    print(f"Voice: {VOICE_NAME}")
    print(f"Locale: {LOCALE}")
    print(f"Workers: {MAX_WORKERS} parallel API clients")
    print(f"Download directory: {DOWNLOAD_DIR.absolute()}")
    print("=" * 60)

    # Split sentences into batches for parallel processing
    sentences_per_worker = len(sentences) // MAX_WORKERS
    batches = []

    for i in range(MAX_WORKERS):
        start_idx = i * sentences_per_worker
        if i == MAX_WORKERS - 1:  # Last worker takes remaining sentences
            end_idx = len(sentences)
        else:
            end_idx = start_idx + sentences_per_worker

        # Create batch with (sentence_number, sentence_text) tuples
        batch_sentences = [(idx + 1, sentences[idx]) for idx in range(start_idx, end_idx)]
        batches.append((i + 1, batch_sentences))

        print(f"Worker {i + 1}: {len(batch_sentences)} sentences (#{start_idx + 1} to #{end_idx})")

    print(f"\nStarting parallel processing...")
    start_time = time.time()

    successful_count = 0
    failed_sentences = []

    # Process batches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all batches to workers
        future_to_worker = {executor.submit(process_sentence_batch, batch): batch[0] for batch in batches}

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            try:
                worker_id, batch_results = future.result()

                # Process results from this worker
                for sentence_num, success, sentence in batch_results:
                    if success:
                        successful_count += 1
                    else:
                        failed_sentences.append((sentence_num, sentence[:50]))

                print(f"Worker {worker_id} completed!")

            except Exception as exc:
                print(f"Worker {worker_id} generated an exception: {exc}")

    # Final results
    total_time = time.time() - start_time

    print(f"\n" + "=" * 60)
    print(f"PARALLEL API PROCESSING COMPLETED!")
    print(f"=" * 60)
    print(f"Results:")
    print(f"  Total sentences: {len(sentences)}")
    print(f"  Successfully processed: {successful_count}")
    print(f"  Failed: {len(failed_sentences)}")
    print(f"  Success rate: {(successful_count / len(sentences) * 100):.1f}%")
    print(f"  Total time: {total_time / 60:.1f} minutes")
    print(f"  Average per sentence: {total_time / len(sentences):.1f} seconds")
    print(
        f"  Effective speed with {MAX_WORKERS} workers: {(total_time / len(sentences)) * MAX_WORKERS:.1f}s per worker")

    if failed_sentences:
        print(f"\nFailed sentences (first 10):")
        for idx, sentence in failed_sentences[:10]:
            print(f"  {idx}: {sentence}...")

    print(f"\nAudio files saved to: {DOWNLOAD_DIR.absolute()}")

    if successful_count > 0:
        print(f"🎉 Successfully generated {successful_count} Telugu audio files!")

    estimated_old_time = len(sentences) * 3  # Your current ~3 seconds per sentence
    time_saved = (estimated_old_time - total_time) / 60
    speed_improvement = (estimated_old_time / total_time) * 100 - 100

    print(f"\n⚡ SPEED IMPROVEMENTS:")
    print(f"   Sequential estimated time: {estimated_old_time / 60:.1f} minutes")
    print(f"   Parallel actual time: {total_time / 60:.1f} minutes")
    print(f"   Time saved: {time_saved:.1f} minutes")
    print(f"   Speed improvement: {speed_improvement:.0f}% faster")

    print("Parallel API processing complete!")


# Test function to verify API works with one sentence
def test_api():
    """Test the API with a single sentence."""
    print("Testing API connection...")

    api_client = MicMonsterAPI()
    test_sentence = "హలో, ఇది ఒక పరీక్ష వాక్యం."

    success = api_client.generate_audio(test_sentence, 0)

    if success:
        print("✅ API test successful!")
        return True
    else:
        print("❌ API test failed!")
        return False


if __name__ == "__main__":
    # First test with one sentence
    if test_api():
        print("\nAPI works! Starting parallel processing...\n")
        main_parallel()
    else:
        print("\nAPI test failed. Check connection and try again.")
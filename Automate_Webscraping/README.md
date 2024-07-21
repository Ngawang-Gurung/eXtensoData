# Webscraping of Voter's Database

## Getting Started

### Step 1: Installation 

1. **Create a Virtual Environment**:
   - Navigate to your project directory and create a virtual environment named `scraping-venv`.
     ```bash
     python -m venv venv
     ```
   - Activate the virtual environment:
     - On Windows:
       ```bash
       venv\Scripts\activate
       ```
     - On macOS/Linux:
       ```bash
       source venv/bin/activate
       ```

2. **Install the packages listed in `requirements.txt`**:
     ```bash
     pip install -r requirements.txt
     ```
3. **Ensure you have the latest version of either Chrome or Firefox installed on your machine and edit the driver line in the script depending on which browser you're using in your script.**

   ```python
   driver = webdriver.Firefox()
   ```

   ```python
   driver = webdriver.Chrome()
   ```
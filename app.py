from flask import Flask, request, jsonify
import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key=GOOGLE_API_KEY)

@app.route('/api/get-multiplier', methods=['POST'])
def get_multiplier():
    try:
        data = request.json
        descriptor = data.get('descriptor', '')
        
        if not descriptor:
            return jsonify({'success': True, 'multiplier': 1.0})

        prompt = f"""Based on the following description of woodchucks' state, provide a multiplier value between 0.2 and 2.0 that represents how much wood they would chuck.

Description: "{descriptor}"

Rules:
- 0.2 to 0.5: Woodchucks are not very motivated (lazy, tired, sad)
- 0.5 to 0.8: Woodchucks are somewhat unmotivated
- 0.8 to 1.2: Woodchucks are neutral/normal
- 1.2 to 1.5: Woodchucks are somewhat motivated
- 1.5 to 2.0: Woodchucks are very motivated (energetic, happy, determined)

Respond with ONLY a single number between 0.2 and 2.0, nothing else. For example: 1.5"""
        
        model = genai.GenerativeModel('gemini-pro')
        response = model.generate_content(prompt)
        
        # Parse the response
        multiplier_text = response.text.strip()
        multiplier = float(multiplier_text)
        
        # Validate the multiplier is in range
        if multiplier < 0.2 or multiplier > 2.0:
            multiplier = 1.0
        
        return jsonify({'success': True, 'multiplier': multiplier})
    
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid multiplier value received from AI'})
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
from app import app, OUTPUTS_DIR

if __name__ == '__main__':
    print(f"\n✅  DataHarvest  →  http://127.0.0.1:5055")
    print(f"📁  Outputs      →  {OUTPUTS_DIR}\n")
    app.run(host='0.0.0.0', port=5055, debug=False)

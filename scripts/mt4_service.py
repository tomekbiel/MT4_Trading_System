import os
import sys
import time
import servicemanager
import win32serviceutil
import win32service
import win32event
import win32api
import win32con
import win32process
import win32profile

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class MT4TradingService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MT4TradingService"
    _svc_display_name_ = "MT4 Trading System Service"
    _svc_description_ = "Runs the MT4 Trading System as a Windows Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = False

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.running = False

    def SvcDoRun(self):
        import threading
        self.running = True
        
        # Start the main script in a separate thread
        def run_script():
            from run_mt4 import main
            main()
        
        thread = threading.Thread(target=run_script)
        thread.daemon = True
        thread.start()
        
        # Keep the service running until stopped
        while self.running:
            time.sleep(1)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        # If no arguments, show the service control dialog
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MT4TradingService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Handle command line arguments (install, start, stop, remove, etc.)
        win32serviceutil.HandleCommandLine(MT4TradingService)

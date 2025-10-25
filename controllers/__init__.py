"""
USPTO Controller-Based Data Processing System

This package contains the modular controller system for processing USPTO trademark data.

Controllers:
- USPTOAPIController: Handles USPTO API interactions
- DownloadController: Manages file downloads and extraction
- ProcessingController: Transforms raw data into structured format
- DatabaseController: Handles database operations and optimization
- USPTOOrchestrator: Coordinates all controllers into a unified process

Usage:
    from controllers.core.uspto_controllers import USPTOOrchestrator
    
    orchestrator = USPTOOrchestrator(config)
    orchestrator.initialize()
    results = orchestrator.run_full_process()
    orchestrator.cleanup()
"""

__version__ = "1.0.0"
__author__ = "USPTO Data Processing Team"

# Import main classes for easy access
from .core.uspto_controllers import (
    USPTOAPIController,
    DownloadController,
    ProcessingController,
    DatabaseController,
    USPTOOrchestrator,
    ProductInfo,
    FileInfo,
    ProcessingResult
)

__all__ = [
    'USPTOAPIController',
    'DownloadController', 
    'ProcessingController',
    'DatabaseController',
    'USPTOOrchestrator',
    'ProductInfo',
    'FileInfo',
    'ProcessingResult'
]

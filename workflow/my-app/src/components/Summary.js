import React from "react";
import { useNavigate } from "react-router-dom";  
import "../App.css";

export default function Summary() {
  const navigate = useNavigate(); 
  return (
    <div className="summary-page">
      <h1 className="summary-title">Workflow Overview</h1>

      <div className="summary-container">
        <div className="workflow-sequence">
          <div className="workflow-box">Technographics Request</div>
          <div className="arrow">⬇</div>

          <div className="workflow-box">Web Scraping Engine (Scrapingdog)</div>
          <div className="arrow">⬇</div>

          <div className="workflow-box">Context Recognition (5 model types)</div>
          <div className="arrow">⬇</div>

          <div className="workflow-box">QC Scripts + Manual Validation</div>
          <div className="arrow">⬇</div>

          <div className="workflow-box">Script Refinement & Feedback</div>
          <div className="arrow loop">↻</div>

          <div className="workflow-box">Final Results</div>
        </div>

        <p className="summary-note">
        </p>

      {/* Arrow Button */}
      <button
        className="arrow-btn"
        onClick={() => navigate("/fifth")}
        aria-label="Go to Next Step"
      >
        ➔
      </button>
    </div>
    </div>
  );
}

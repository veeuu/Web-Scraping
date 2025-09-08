import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Second() {
  const navigate = useNavigate();

  return (
    <div className="App">
      <div className="workflow-step">
        <h1 className="step-heading">Workflow – Step 1</h1>
        <p className="step-text">Incoming Request Handling</p>

        <h3 className="step-heading">Source:</h3>
        <p className="step-text">Technographics Team</p>

        <h3 className="step-heading">Types of Requests:</h3>
        <ul className="step-list">
          <li>Only Firmographics</li>
          <li>Only Technographics</li>
          <li>Combined Firmographics + Technographics</li>
        </ul>
      </div>

      {/* Arrow Button */}
      <button
        className="arrow-btn"
        onClick={() => navigate("/third")}
        aria-label="Go to Next Step"
      >
        ➔
      </button>
    </div>
  );
}

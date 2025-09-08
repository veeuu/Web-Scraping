import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Sixth() {
  const navigate = useNavigate();

  return (
    <div className="sixth-page">
      <div className="workflow-step-6">
        <h1 className="step-heading-6">Workflow – Step 5</h1>
        <p className="step-text-6">Script Refinement & Feedback Loop</p>

        <h3 className="step-heading-6">Goal:</h3>
        <p className="step-text-6">
          Address QC gaps and improve accuracy
        </p>

        <h3 className="step-heading-6">If a model fails:</h3>
        <ul className="step-list-6">
          <li>Diagnose error source</li>
          <li>Improve prompts or adjust keyword handling</li>
          <li>Iterate with team collaboration</li>
        </ul>
      </div>

      {/* Arrow Button OUTSIDE the container */}
      <button
        className="arrow-btn"
        onClick={() => navigate("/seventh")}
        aria-label="Go to Next Step"
      >
        ➔
      </button>
    </div>
  );
}

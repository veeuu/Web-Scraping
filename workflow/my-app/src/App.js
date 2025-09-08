import React from "react";
import { BrowserRouter as Router, Routes, Route, useLocation } from "react-router-dom";
import First from "./components/First";
import Second from "./components/Second";
import Third from "./components/Third";
import Fourth from "./components/Fourth";
import Fifth from "./components/Fifth";
import Sixth from "./components/Sixth";
import Seventh from "./components/Seventh";
import Eighth from "./components/Eighth";
import Summary from "./components/Summary";
import ThankYou from "./components/ThankYou";
import Header from "./components/Header";
import "./App.css";

function Layout() {
  const location = useLocation();
  return (
    <>
      {}
      {location.pathname !== "/" && location.pathname !== "/thankyou" && <Header />}

      <Routes>
        <Route path="/" element={<First />} />
        <Route path="/second" element={<Second />} />
        <Route path="/third" element={<Third />} />
        <Route path="/fourth" element={<Fourth />} />
        <Route path="/fifth" element={<Fifth />} />
        <Route path="/sixth" element={<Sixth />} />
        <Route path="/seventh" element={<Seventh />} />
        <Route path="/eighth" element={<Eighth />} />
        <Route path="/summary" element={<Summary />} />
        <Route path="/thankyou" element={<ThankYou />} />
      </Routes>
    </>
  );
}

export default function App() {
  return (
    <Router>
      <Layout />
    </Router>
  );
}

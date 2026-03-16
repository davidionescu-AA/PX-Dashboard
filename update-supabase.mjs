import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";

// Parse .env
const envContent = readFileSync(".env", "utf8");
const env = {};
for (const line of envContent.split("\n")) {
  const idx = line.indexOf("=");
  if (idx > 0) env[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
}

const supabase = createClient(env.VITE_SUPABASE_URL, env.SUPABASE_SERVICE_KEY);

const now = new Date().toISOString();

const INSIGHTS = [
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "critical",
    icon: "🧪",
    title: "MAP Screener Technical Failures Were the #1 CX Crisis This Period",
    priority: "P1",
    status: "published",
    generated_at: now,
    body: "MAP Diagnostic/Screener generated 85 conversations across 4 weeks, making it the single largest sub-category. 61 of those 85 (72%) came from families in the enrollment funnel. The Mar 2 week was the peak: 33 screener conversations, 26 from enrolling families. That is 30.2% of all enrollment-funnel conversations that week. These were families whose first experience with Alpha was a broken test. The spike was caused by two overlapping events: the enrollment funnel opening (more families entering) and a technical issue with the screener itself. Once the technical issue was resolved, screener volume dropped from 26 (Mar 2) to 7 (Mar 9). This is distinct from MAP Growth Testing (51 convos), which is post-enrollment follow-up. The screener issue hit families at the highest-stakes moment in the funnel. Critically, the support team has no ability to fix screener technical issues themselves. When a family reports a broken screener, the team can only escalate to engineering and wait. If engineering is slow to respond, a family can sit for 1-3 days with no resolution and a dead enrollment funnel. During this period the support team has their hands bound: they are managing the parent's frustration without being able to solve the problem.",
    actions: JSON.stringify([
      "Before the next MAP window: pre-build a screener troubleshooting guide covering the top 3 failure modes (can't log in, test won't load, scores not appearing). Deploy as a proactive email to every family within 1 hour of screener assignment.",
      "Track conversion rate of the 26 Mar 2 screener families separately. If their conversion is measurably lower than other cohorts, that is the dollar cost of technical failures in the funnel.",
      "Establish an engineering SLA for screener issues during active MAP windows. The current situation where a family can wait 1-3 days for a fix that is entirely outside the support team's control is unacceptable for enrollment-stage families. Support needs a guaranteed turnaround from engineering during these windows.",
      "Every screener failure costs approximately 1 support conversation (avg 13 messages) and risks losing a family permanently. Prioritize screener reliability as a product/engineering investment.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "warning",
    icon: "📉",
    title: "15 Cancellations in 4 Weeks, Accelerating Post-MAP, Most Churning Silently",
    priority: "P1",
    status: "published",
    generated_at: now,
    body: "15 real cancellations (excluding team member conversations) over the 4-week period. Distribution: Feb 16=2, Feb 23=2, Mar 2=7, Mar 9=4. The Mar 2 spike aligns with post-MAP assessment decisions. Reading every cancellation conversation, the reasons cluster into three buckets: (1) child not engaging independently or not the right fit (Veronica Max, Leah Neal, Kara Porterfield, Ashley Mahoney), (2) technical frustrations that accumulated over time (Aleesha Greene with broken laptops and StudyReel issues, David Murray citing login friction across multiple systems), (3) child struggling emotionally with the curriculum (Faith Bowen reporting daily tears). The most concerning signal: 11 of 14 cancelling families had zero prior support contact before their cancellation message. They churned silently with no early warning. The acceleration from 2 per week to 5.5 per week needs monitoring. Cancellation handling quality varies significantly across the team: some conversations include genuine retention effort (open questions, call offers, addressing the specific concern), others are processed administratively with no attempt to understand the reason. All three agents have examples of both strong and weak cancellation handling. The difference is not skill but consistency: there is no minimum standard.",
    actions: JSON.stringify([
      "Create a 3-tier cancellation handling protocol. Tier 1 (formal cancellation request using legal language like 'treat this as a cancellation'): process immediately, you cannot gate cancellation behind questions, but offer a conversation after processing. Tier 2 (soft signal like 'not working' or 'not the right fit'): ask one open question first, explore whether the problem is solvable before processing. Tier 3 (distress signal like child crying, anxiety, emotional struggle): this should trigger a call or detailed engagement with the child's specific situation before any processing.",
      "Implement a silent churn early warning: if a student's learning time drops below threshold for 5+ consecutive days and the family has not contacted support, trigger a proactive check-in email from the assigned agent.",
      "Tag every cancellation with primary reason (engagement, technical, financial, emotional) to build a churn reason database. After 3 months this data will show which bucket is largest and where to invest.",
      "Make David's cancellation response to David Murray the team reference for Tier 2 handling: acknowledge the feedback point by point, take ownership, explain what is being done, offer a call, leave the door open. Make Mary's response to Amanda Nigg the reference for how a cancellation can turn into a relationship-preserving conversation when the agent asks what happened.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "warning",
    icon: "⏱️",
    title: "Mary's RT Is Bimodal: Fastest Median (43 min) but Worst P90 (667 min)",
    priority: "P1",
    status: "published",
    generated_at: now,
    body: "Business-hours-only RT (6am-6pm EST weekdays) reveals a split on the team. Mary Betz: median 43.1 min, mean 238.9 min, P90 666.6 min. Nene Addico: median 84.7 min, mean 137.4 min, P90 289.9 min. David Ionescu: median 95.3 min, mean 125.0 min, P90 284.3 min. Mary's gap between median and P90 (43 vs 667 min) is by far the largest. This is explained by her live session and other work schedule: when she is in a call or working on other projects, conversations wait; when she is free, she blitzes through the queue. 24.3% of her intra-conversation response pairs take over 12 hours. David and Nene have tighter distributions (David's median-to-P90 ratio is 1:3, Mary's is 1:15), meaning families assigned to David or Nene get a more predictable experience. All three team members have significant work beyond conversation handling: David builds tooling and dashboards, Mary runs live sessions, Nene handles operational tasks. David's slower median is also partially driven by his Bucharest timezone (UTC+2). The P90 gap is the metric that matters: it reflects the experience of the families who wait the longest.",
    actions: JSON.stringify([
      "Track P90 RT as a team KPI alongside median. Median hides the tail. A family whose conversation sits for 11 business hours has a fundamentally different experience than one who gets a response in 43 minutes.",
      "Consider reducing Mary's live session load during high-volume weeks (Mar 2 was 13.6 convos/day for her while also doing calls). The bimodal pattern is a scheduling problem, not a performance problem.",
      "Implement a daily check: any conversation without a response for more than 48 business hours gets flagged and either resolved, escalated, or reassigned. Do not let conversations age silently.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "info",
    icon: "👥",
    title: "Agent Communication Styles Vary Significantly: Standardize the Floor, Not the Ceiling",
    priority: "P2",
    status: "published",
    generated_at: now,
    body: "Analysis of 1,400+ agent messages reveals distinct styles. David Ionescu (signs as Rowan): median 76 words per message, 31.4% in the 80-150 word range, only 17.0% under 30 words. He writes the most thorough, consultative responses on the team. His first responses to enrolling families average 94-140 words with full context and proactive information. Mary Betz: median 54 words per message, 30.2% under 30 words. She is the most action-oriented and technical: highest rate of including direct links to resources (10.6% of messages) and referencing specific tools by name (IXL, Lalilo, Khan, Zearn, StudyReel) in 12.9% of messages. She resolves technical issues faster than anyone. Nene Addico (signs as Leland): median 53 words per message, 31.3% under 30 words. Most structured and formal in tone. His first responses to enrolling families are noticeably shorter (23-76 words vs David's 94-140 words) and more procedural. All three styles have strengths in different contexts, but for enrolling families evaluating a $833/month program, the experience varies dramatically depending on which agent the round-robin assigns. The quality ceiling is fine on all three agents. The floor is where the gap is.",
    actions: JSON.stringify([
      "Create a minimum standard for first responses to enrolling families: at least 60-80 words, directly answer the question, include one piece of unrequested helpful context, and a clear next step. Let agents personalize tone above this floor.",
      "Have David run a 30-minute session with Nene on enrolling-family first-response structure. Goal is not to make Nene write 140 words, but to ensure every first response includes answer + reassurance + next step.",
      "Nene should include more links and direct resources in messages (currently 2.3% vs Mary's 10.6%). A link to a help article or video resolves faster than a text explanation and gives the family something tangible.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "info",
    icon: "🌍",
    title: "Global Team Timezone Coverage Is an Asset but Creates Invisible Costs",
    priority: "P2",
    status: "published",
    generated_at: now,
    body: "The team spans three timezones: David in Bucharest (UTC+2), Nene in Ghana (UTC+0), Mary in the US (EST). David's 6am EST responses = 1pm Bucharest. His peak at 3-4pm EST = 10-11pm local. Nene's 7am EST start = noon Ghana; his 5-6pm EST wind-down = 10-11pm local. Both are working afternoon-to-late-night local time to cover the US family window. Mary is the only one on natural time. Current effective coverage: 7am-7pm EST with all three agents overlapping from approximately 10am-5pm EST. 38% of inbound (250 of 659 conversations) arrives outside 6am-6pm EST business hours, with the heaviest window at 5-9pm EST. This is when parents get home from work and hear about problems from their kids. The after-hours inbound is not a coverage problem to solve by extending hours. It is a relay-delay problem: the kid hit the issue at 10am, waited until the parent got home, and the parent wrote at 8pm. The kid-direct-contact strategy is the structural fix.",
    actions: JSON.stringify([
      "Do not ask David or Nene to extend hours further. They are already working until 10-11pm local time. If coverage needs to expand, it should come from automation (chatbot) or a fourth hire.",
      "The kid-direct-contact strategy (already underway) is the structural fix for the after-hours gap. If kids message during school hours (8am-3pm EST) instead of parents messaging at 8pm, inbound shifts into the fully-covered window.",
      "Monitor whether the student-direct channel shifts the inbound hour curve over the next 4-8 weeks. If it does, after-hours volume should decrease naturally.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "positive",
    icon: "🎓",
    title: "Student Direct Contact Channel More Than Doubled: 57 Student Conversations, 23 in Mar 9 Alone",
    priority: "P2",
    status: "published",
    generated_at: now,
    body: "Combining the Students source channel with @2hourlearning.com student emails (excluding staff), there were 57 student conversations across 4 weeks. By week: Feb 16=9, Feb 23=13, Mar 2=12, Mar 9=23. The Mar 9 week shows a clear inflection from the deliberate strategy change pushing students to contact support directly. Student conversations are heavily Curriculum & Learning focused (47%), mostly asking how-do-I questions: \"whats the password for class bank,\" \"how do I earn alpha bucks,\" \"trying to get alphas,\" \"logging into Lalilo.\" These are exactly the type of questions that should come from the person experiencing the problem rather than being relayed through a parent hours later. Important routing note: 25 of 57 student conversations were routed to the wrong source channel (17 to Customers, 8 to Prospects) because the routing logic does not recognize @2hourlearning.com emails as students. This means student volume has been undercounted in all prior reporting.",
    actions: JSON.stringify([
      "Fix the routing: @2hourlearning.com emails (excluding staff) should automatically route to the Students channel. 25 of 57 student conversations are currently miscategorized, which distorts both student and customer/prospect volume numbers.",
      "Track student vs parent inbound ratio weekly. Target: student channel should grow to 30-40% of curriculum and technical questions within 8 weeks.",
      "Ensure the AI chatbot (in development) is calibrated for 10-14 year olds. Short sentences, simple language, visual guides. If the chatbot frustrates a kid, they will close it and tell their parent at dinner, reversing the entire strategy.",
      "Student how-do-I questions are the highest-value chatbot target. If the bot handles 'where do I find X' and 'my lesson won't load,' agents only get the questions that actually need a human.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "warning",
    icon: "📊",
    title: "Team Is at Capacity: The Decision Is Hire, Automate, or Accept the Tradeoff",
    priority: "P1",
    status: "published",
    generated_at: now,
    body: "659 conversations across 4 weeks on a 3-person team. Average: approximately 44 conversations per person per week, or 9 per day. Peak loads: Nene hit 14.6 convos/day during Mar 2, Mary hit 13.6/day. All three team members have significant work beyond conversation handling (building tools, running live sessions, operational projects, dashboard development), and that work has real value for the organization. At peak volume, conversations pile up and the team-wide rate of 12+ hour response gaps stays above 22% for every agent. The Mar 2 spike (194 conversations, 27% above average) was absorbed but at the cost of consistency. The decision facing the team is a three-way tradeoff. Option 1: bring on a fourth team member to handle conversation volume. Option 2: accelerate automation (chatbot, workflows) to deflect volume, but building automation itself requires team member time, which they do not have at current volume. Option 3: reduce team members' time on other projects to free up conversation capacity, but the tools, dashboards, and processes they build are what makes the operation scalable, so pulling them off that work has significant downsides. This is not a problem that resolves itself. Volume will spike again at the next funnel push or MAP window.",
    actions: JSON.stringify([
      "Make a clear decision on which path (hire, automate, or reallocate) to pursue before the next volume spike. Do not wait for quality to degrade to make the call.",
      "If automating: get a specific go-live date from the agency building the chatbot. If the chatbot is not ready before the next funnel push, you need a contingency.",
      "If hiring: a fourth person in a US timezone would add natural-time coverage during the 10am-7pm EST window when volume peaks, and would free up existing team members to continue building the tools that make the operation scalable.",
      "If reallocating: be specific about which projects pause and for how long. An open-ended reduction in project work will quietly erode the team's ability to build the infrastructure that reduces future volume.",
      "After the chatbot launches, re-benchmark RT and workload. The remaining human conversations will be harder and take longer. Do not compare post-chatbot RT to pre-chatbot RT without adjusting for complexity.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "info",
    icon: "🔁",
    title: "33% Repeat Contact Rate Signals Unresolved Issues and Deflection Opportunities",
    priority: "P2",
    status: "published",
    generated_at: now,
    body: "125 of 379 unique users (33%) contacted support more than once during the 4-week period. Distribution: 62 users had 2 conversations, 30 had 3, 17 had 4, and 16 users had 5 or more. Heavy repeaters (5+ conversations, excluding team members) cluster in Curriculum & Learning (31 convos), Platform & Technical (17), and Account & Billing (10). These are families who are either confused by the same things repeatedly, not getting full resolution on first contact, or experiencing ongoing product friction. Top repeat contacts include Lauren Cole (14 convos), Tanya Wansom (9), and several families at 7 each.",
    actions: JSON.stringify([
      "When a family reaches 4+ conversations in a 2-week window, send them a proactive email with a full outline of where their child is at: current levels, progress since enrollment, upcoming milestones, and answers to the most common questions for their stage. This shows the family they are seen and often pre-answers the questions driving the repeat contacts.",
      "Audit the top 10 repeat contacts to determine whether they are product issues (broken features, confusing UI), knowledge gaps (same question asked differently), or families who need a more structured onboarding. Each type needs a different intervention.",
      "Track first-contact resolution rate as a KPI. If a conversation is reopened within 48 hours, the first contact did not resolve the underlying issue.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "info",
    icon: "📂",
    title: "Deflection Priority Stack for Chatbot Development",
    priority: "P2",
    status: "published",
    generated_at: now,
    body: "Category analysis reveals a clear priority order for chatbot deflection. Tier 1 (highest deflectability, build first): Login & Credentials (20 convos, nearly 100% automatable), Alpha Rewards status and redemption (35 convos, mostly lookup-based), MAP Screener FAQ-type questions (estimated 40-50 of 85 screener convos are how-do-I-start and what-does-my-score-mean, not technical failures), Apps & Software navigation (portion of 64 convos, where-do-I-find-X). Tier 2 (partially automatable): Onboarding step-by-step guidance (49 convos), Billing & Subscription status lookups (19 convos), Student Progress data surfacing (36 convos). Tier 3 (do NOT automate, keep human): Cancellations (15 convos, need human with tiered protocol), Coaching & Scheduling (35 convos, relationship-dependent), any enrolling family in their first week (first impressions too important). Categories requiring the most messages to resolve: Coaching & Scheduling (13.6 avg msgs), Testing & Assessment (13.0), Enrollment & Onboarding (11.2). The lightest: General & Other (7.8), Rewards (8.1).",
    actions: JSON.stringify([
      "Share this priority stack with the agency building the chatbot. Tier 1 items should be the first modules deployed.",
      "For MAP Screener specifically: the chatbot needs to distinguish between 'I have a question about my scores' (deflectable) and 'the test won't load or I am getting an error' (not deflectable, route to human immediately). Misrouting a technical failure to a bot loop will make the family experience worse, not better.",
      "After chatbot launch, measure deflection rate per category weekly. Target: 70%+ deflection on Tier 1 categories within the first month.",
      "Conservative estimate: a well-built chatbot should deflect 80-100 conversations per month from Tier 1 categories alone. That is roughly 3 conversations per day that never reach a human, or about 12-15% of total current volume.",
    ]),
  },
  {
    week_label: "w/c Mar 9",
    insight_type: "signal",
    severity: "info",
    icon: "🏷️",
    title: "58% Customers, 37% Enrolling Families, 9% Students: Enrolling Family Experience Needs Parity",
    priority: "P3",
    status: "published",
    generated_at: now,
    body: "Source channel breakdown across 4 weeks (corrected for student misrouting): Customers 385 (58%), Enrolling Families 241 (37%), Students 57 (corrected, 9%, previously undercounted at 32). Enrolling-family volume spiked 59% in Mar 2 (86, up from avg 52) driven by funnel opening plus screener crisis, then dropped to 40 in Mar 9 once the technical issue resolved. Nene handles the highest share of enrolling families (101 of his 231 convos = 43.7%) vs Mary (65/221 = 29.4%) and David (73/201 = 36.3%). This is a round-robin effect, not intentional routing. Enrolling-family conversations average 13.2 messages each (vs 10.9 overall), meaning they take more effort per conversation. First responses to enrolling families vary significantly by agent: David averages 94-140 words with full context, Nene averages 23-76 words and is more procedural. For a program at this price point, the first support interaction with an enrolling family is a conversion moment.",
    actions: JSON.stringify([
      "Monitor enrolling-family conversion by assigned agent. If there is a measurable difference, it justifies investing in specific response training.",
      "Consider a dedicated enrolling-family response template that all agents use for first touch, ensuring consistent quality regardless of round-robin assignment.",
      "As the student channel grows, rebalance priorities: students should get fast, simple responses (chatbot-first), enrolling families should get the most consultative human responses, existing customers fall in between.",
    ]),
  },
];

async function main() {
  console.log("Starting Supabase update...\n");

  // 1. Delete ALL existing insights
  console.log("1. Deleting all existing weekly_insights...");
  const { error: delErr, count } = await supabase
    .from("weekly_insights")
    .delete()
    .not("id", "is", null);
  if (delErr) {
    console.error("   Delete error:", delErr.message);
    return;
  }
  console.log(`   Deleted existing insights.`);

  // 2. Insert 10 new insights
  console.log("2. Inserting 10 new insights...");
  const { error: insErr } = await supabase.from("weekly_insights").insert(INSIGHTS);
  if (insErr) {
    console.error("   Insert error:", insErr.message);
    return;
  }
  console.log("   Inserted 10 insights successfully.");

  // 3. Update weekly_stats for w/c Mar 9
  console.log("3. Updating weekly_stats for w/c Mar 9...");
  const agentStats = JSON.stringify([
    { name: "David Ionescu", conversations: 49, messages: 490, median_response_min: 97.7, p25: 15.0, p75: 179.0, p90: 284.3 },
    { name: "Mary Betz", conversations: 63, messages: 584, median_response_min: 37.6, p25: 2.5, p75: 323.7, p90: 666.6 },
    { name: "Nene Addico", conversations: 45, messages: 400, median_response_min: 61.4, p25: 10.0, p75: 227.1, p90: 289.9 },
  ]);

  const statsUpdate = {
    total_conversations: 157,
    total_messages: 1474,
    agent_stats: agentStats,
    computed_at: now,
  };

  // Check if the row exists
  const { data: existing } = await supabase
    .from("weekly_stats")
    .select("id")
    .eq("week_label", "w/c Mar 9");

  if (existing && existing.length > 0) {
    const { error } = await supabase
      .from("weekly_stats")
      .update(statsUpdate)
      .eq("id", existing[0].id);
    if (error) {
      console.error("   Update error:", error.message);
      return;
    }
    console.log(`   Updated weekly_stats row id=${existing[0].id}`);
  } else {
    const { error } = await supabase
      .from("weekly_stats")
      .insert({ week_label: "w/c Mar 9", ...statsUpdate });
    if (error) {
      console.error("   Insert error:", error.message);
      return;
    }
    console.log("   Inserted new weekly_stats row for w/c Mar 9");
  }

  console.log("\nDone! All updates applied successfully.");
}

main().catch(console.error);

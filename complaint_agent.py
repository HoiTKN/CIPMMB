import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from huggingface_hub import login
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("complaint_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ComplaintAgent")

class HuggingFaceComplaintAgent:
    """AI agent for analyzing customer complaint data using Hugging Face models"""
    
    def __init__(self, model_name="mistralai/Mistral-7B-Instruct-v0.2", knowledge_base_path="complaint_knowledge_base.json", token=None):
        """
        Initialize the complaint analytics agent
        
        Args:
            model_name: Name of the Hugging Face model to use
            knowledge_base_path: Path to the complaint knowledge base JSON file
            token: Hugging Face API token for accessing gated models
        """
        self.model_name = model_name
        self.knowledge_base_path = knowledge_base_path
        self.df = None
        self.load_knowledge_base()
        
        # Authenticate with Hugging Face if token is provided
        if token:
            try:
                logger.info("Authenticating with Hugging Face")
                login(token=token)
                logger.info("Successfully authenticated with Hugging Face")
            except Exception as e:
                logger.error(f"Error authenticating with Hugging Face: {str(e)}")
        
        # Setup model
        self.setup_model()
        logger.info(f"Complaint Analytics Agent initialized with model: {model_name}")
    
    def load_knowledge_base(self):
        """Load complaint data from knowledge base"""
        try:
            if os.path.exists(self.knowledge_base_path):
                with open(self.knowledge_base_path, 'r') as f:
                    self.knowledge_base = json.load(f)
                
                # Convert to DataFrame for easier analysis
                self.df = pd.DataFrame(self.knowledge_base["complaints"])
                logger.info(f"Loaded {len(self.df)} complaints from knowledge base")
                
                # Convert date columns to datetime
                date_columns = ["Ngày SX", "Ngày tiếp nhận"]
                for col in date_columns:
                    if col in self.df.columns:
                        self.df[col] = pd.to_datetime(self.df[col], format="%d/%m/%Y", errors='coerce')
                
                # Ensure numeric columns are properly typed
                numeric_columns = ["SL pack/ cây lỗi", "Line", "Máy"]
                for col in numeric_columns:
                    if col in self.df.columns:
                        if col == "SL pack/ cây lỗi":
                            # Special handling for problematic column
                            self.df[col] = pd.to_numeric(self.df[col].replace('', np.nan), errors='coerce').fillna(0)
                        else:
                            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                
                # Fix the problematic 'Số lượng (ly/hộp/chai/gói/hủ)' column if present
                if "Số lượng (ly/hộp/chai/gói/hủ)" in self.df.columns:
                    self.df["Số lượng (ly/hộp/chai/gói/hủ)"] = pd.to_numeric(
                        self.df["Số lượng (ly/hộp/chai/gói/hủ)"].replace('', np.nan), 
                        errors='coerce'
                    ).fillna(0)
            else:
                logger.warning(f"Knowledge base file not found: {self.knowledge_base_path}")
                self.df = pd.DataFrame()
                self.knowledge_base = {"complaints": [], "metadata": {}}
        except Exception as e:
            logger.error(f"Error loading knowledge base: {str(e)}")
            self.df = pd.DataFrame()
            self.knowledge_base = {"complaints": [], "metadata": {}}
    
    def setup_model(self):
        """Set up the Hugging Face model"""
        try:
            logger.info(f"Loading model: {self.model_name}")
            
            # Try to load primary model - Mistral-7B (or specified model)
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
                
                # Check if enough resources are available
                available_memory = torch.cuda.get_device_properties(0).total_memory if torch.cuda.is_available() else 0
                
                # For smaller models or if enough resources
                if "7B" not in self.model_name or available_memory > 14e9:  # 14GB
                    self.model = AutoModelForCausalLM.from_pretrained(
                        self.model_name, 
                        torch_dtype=torch.float16,
                        device_map="auto",
                        low_cpu_mem_usage=True
                    )
                    # Create text generation pipeline
                    self.pipe = pipeline(
                        "text-generation",
                        model=self.model,
                        tokenizer=self.tokenizer,
                        max_length=2048,
                        temperature=0.1,
                        top_p=0.95,
                        repetition_penalty=1.15
                    )
                    logger.info(f"Successfully loaded primary model: {self.model_name}")
                else:
                    # Fall back to smaller model if not enough resources
                    logger.warning(f"Insufficient resources for {self.model_name}. Falling back to smaller model")
                    raise ValueError("Insufficient resources")
            
            except Exception as e:
                logger.warning(f"Could not load primary model: {str(e)}. Trying fallback model...")
                
                # Try with TinyLlama (much smaller model)
                try:
                    fallback_model = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"
                    logger.info(f"Attempting to load fallback model: {fallback_model}")
                    
                    self.tokenizer = AutoTokenizer.from_pretrained(fallback_model)
                    self.model = AutoModelForCausalLM.from_pretrained(
                        fallback_model,
                        device_map="auto",
                        low_cpu_mem_usage=True
                    )
                    
                    # Create text generation pipeline
                    self.pipe = pipeline(
                        "text-generation",
                        model=self.model,
                        tokenizer=self.tokenizer,
                        max_length=2048,
                        temperature=0.1,
                        top_p=0.95,
                        repetition_penalty=1.15
                    )
                    self.model_name = fallback_model
                    logger.info(f"Successfully loaded fallback model: {fallback_model}")
                
                except Exception as e2:
                    # Try with even smaller T5 model if TinyLlama fails
                    logger.warning(f"Could not load TinyLlama: {str(e2)}. Trying smaller T5 model...")
                    try:
                        small_model = "google/flan-t5-small"
                        logger.info(f"Attempting to load small model: {small_model}")
                        
                        self.pipe = pipeline(
                            "text2text-generation",
                            model=small_model,
                            device_map="auto"
                        )
                        self.model_name = small_model
                        logger.info(f"Successfully loaded small model: {small_model}")
                    
                    except Exception as e3:
                        logger.error(f"All model loading attempts failed. Last error: {str(e3)}")
                        self.pipe = None
        
        except Exception as e:
            logger.error(f"Unexpected error in setup_model: {str(e)}")
            self.pipe = None
    
    def _format_prompt(self, instruction, context=""):
        """Format prompt for the model based on model type"""
        if "mistral" in self.model_name.lower():
            # Mistral instruction format
            return f"<s>[INST] {instruction}\n\n{context} [/INST]"
        elif "llama" in self.model_name.lower() or "tinyllama" in self.model_name.lower():
            # Llama/TinyLlama instruction format
            return f"<s>[INST] {instruction} [/INST] {context}</s>"
        elif "flan-t5" in self.model_name.lower():
            # T5 format
            return f"Instruction: {instruction}\n\nContext: {context}"
        else:
            # Generic format
            return f"Instruction: {instruction}\n\nContext: {context}\n\nResponse:"
    
    def _generate_response(self, prompt, max_new_tokens=1024):
        """Generate a response from the model"""
        if self.pipe is None:
            return "Model not available. Please check logs for errors."
            
        try:
            logger.info("Generating response")
            
            # Adjust based on model type
            if "flan-t5" in self.model_name.lower():
                outputs = self.pipe(
                    prompt,
                    max_length=max_new_tokens,
                    do_sample=True
                )
                
                # Extract the generated text - T5 models have different output format
                if isinstance(outputs, list) and len(outputs) > 0:
                    if isinstance(outputs[0], dict) and "generated_text" in outputs[0]:
                        response = outputs[0]["generated_text"]
                    else:
                        response = outputs[0]
                else:
                    response = str(outputs)
            else:
                outputs = self.pipe(
                    prompt, 
                    max_new_tokens=max_new_tokens,
                    do_sample=True,
                    temperature=0.1,
                    top_p=0.95,
                    repetition_penalty=1.15,
                    return_full_text=False
                )
                
                # Extract the generated text
                if isinstance(outputs, list) and len(outputs) > 0:
                    if isinstance(outputs[0], dict) and "generated_text" in outputs[0]:
                        response = outputs[0]["generated_text"]
                    else:
                        response = outputs[0]
                else:
                    response = str(outputs)
            
            # Clean up the response
            response = response.strip()
            return response
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            return f"Error generating response: {str(e)}"
    
    def identify_patterns(self, days_back=30):
        """
        Identify patterns in recent customer complaints
        
        Args:
            days_back: Number of days to look back for recent complaints
            
        Returns:
            JSON string with identified patterns
        """
        if self.df.empty:
            return json.dumps({"error": "No complaint data available"})
        
        try:
            # Prepare statistical analysis for the model
            # Filter for recent complaints
            if 'Ngày tiếp nhận' in self.df.columns:
                recent_date = datetime.now() - timedelta(days=days_back)
                recent_complaints = self.df[self.df['Ngày tiếp nhận'] >= recent_date].copy()
            else:
                recent_complaints = self.df.copy()
                
            if recent_complaints.empty:
                return json.dumps({"error": f"No complaints in the last {days_back} days"})
            
            # Calculate key statistics
            complaint_stats = {}
            
            # Top products by complaints
            if 'Tên sản phẩm' in recent_complaints.columns:
                top_products = recent_complaints['Tên sản phẩm'].value_counts().head(5)
                complaint_stats["top_products"] = top_products.to_dict()
            
            # Top defect types
            if 'Tên lỗi' in recent_complaints.columns:
                top_defects = recent_complaints['Tên lỗi'].value_counts().head(5)
                complaint_stats["top_defects"] = top_defects.to_dict()
            
            # Top lines
            if 'Line' in recent_complaints.columns:
                top_lines = recent_complaints['Line'].value_counts().head(3)
                complaint_stats["top_lines"] = top_lines.to_dict()
            
            # Product-defect combinations
            if all(col in recent_complaints.columns for col in ['Tên sản phẩm', 'Tên lỗi']):
                product_defect_counts = recent_complaints.groupby(['Tên sản phẩm', 'Tên lỗi']).size()
                top_combos = product_defect_counts.nlargest(5)
                complaint_stats["product_defect_combos"] = {f"{prod}-{defect}": count 
                                                       for (prod, defect), count in top_combos.items()}
            
            # Generate prompt for the model
            instruction = """
            You are a QA manager in an FMCG factory analyzing customer complaints. 
            Identify clear patterns and insights from the customer complaint statistics below.
            Focus on:
            1. Which products and defect types appear most frequently?
            2. Are there specific product-defect combinations that stand out?
            3. Are certain production lines associated with more complaints?
            4. What actionable insights can be derived from these patterns?
            
            Provide your response in a concise, analytical format with clear recommendations.
            """
            
            context = f"""
            Customer complaint statistics from the past {days_back} days:
            
            Total complaints analyzed: {len(recent_complaints)}
            
            Top products by complaint count:
            {json.dumps(complaint_stats.get("top_products", {}), indent=2)}
            
            Top defect types:
            {json.dumps(complaint_stats.get("top_defects", {}), indent=2)}
            
            Top production lines with complaints:
            {json.dumps(complaint_stats.get("top_lines", {}), indent=2)}
            
            Top product-defect combinations:
            {json.dumps(complaint_stats.get("product_defect_combos", {}), indent=2)}
            """
            
            # Generate the analysis using the model
            prompt = self._format_prompt(instruction, context)
            analysis = self._generate_response(prompt)
            
            # Compile results
            results = {
                "statistics": complaint_stats,
                "ai_analysis": analysis,
                "date_range": {
                    "days_analyzed": days_back,
                    "from_date": recent_date.strftime("%Y-%m-%d") if 'Ngày tiếp nhận' in self.df.columns else "all dates",
                    "to_date": datetime.now().strftime("%Y-%m-%d")
                }
            }
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error identifying patterns: {str(e)}")
            return json.dumps({"error": str(e)})
    
    def detect_anomalies(self):
        """
        Detect anomalies in the complaint data
        
        Returns:
            JSON string with detected anomalies
        """
        if self.df.empty:
            return json.dumps({"error": "No complaint data available"})
            
        try:
            # Calculate basic statistics for anomaly detection
            anomaly_data = {}
            
            # 1. Products with unusually high defect rates
            if all(col in self.df.columns for col in ['Tên sản phẩm', 'SL pack/ cây lỗi']):
                product_defects = self.df.groupby('Tên sản phẩm')['SL pack/ cây lỗi'].agg(['mean', 'count'])
                product_defects = product_defects[product_defects['count'] >= 5]  # At least 5 complaints
                
                # Find products with defect rates > 2 standard deviations
                overall_mean = product_defects['mean'].mean()
                overall_std = product_defects['mean'].std()
                
                if overall_std > 0:
                    threshold = overall_mean + (2 * overall_std)
                    high_defect_products = product_defects[product_defects['mean'] > threshold]
                    
                    anomaly_data["high_defect_products"] = {
                        product: {
                            "mean_defects": float(row['mean']),
                            "complaint_count": int(row['count']),
                            "overall_average": float(overall_mean),
                            "z_score": float((row['mean'] - overall_mean) / overall_std)
                        }
                        for product, row in high_defect_products.iterrows()
                    }
            
            # 2. Production lines with unusual defect patterns
            if all(col in self.df.columns for col in ['Line', 'SL pack/ cây lỗi']):
                line_defects = self.df.groupby('Line')['SL pack/ cây lỗi'].agg(['mean', 'count'])
                line_defects = line_defects[line_defects['count'] >= 3]  # At least 3 complaints
                
                line_mean = line_defects['mean'].mean()
                line_std = line_defects['mean'].std()
                
                if line_std > 0:
                    line_threshold = line_mean + (2 * line_std)
                    high_defect_lines = line_defects[line_defects['mean'] > line_threshold]
                    
                    anomaly_data["high_defect_lines"] = {
                        str(line): {
                            "mean_defects": float(row['mean']),
                            "complaint_count": int(row['count']),
                            "overall_average": float(line_mean),
                            "z_score": float((row['mean'] - line_mean) / line_std)
                        }
                        for line, row in high_defect_lines.iterrows()
                    }
            
            # 3. Unusual product-line combinations
            if all(col in self.df.columns for col in ['Tên sản phẩm', 'Line', 'Mã ticket']):
                # Get actual counts
                prod_line_counts = self.df.groupby(['Tên sản phẩm', 'Line']).size()
                
                # Get expected counts based on marginals
                prod_counts = self.df.groupby('Tên sản phẩm').size()
                line_counts = self.df.groupby('Line').size()
                total = len(self.df)
                
                unusual_combos = {}
                
                for (prod, line), count in prod_line_counts.items():
                    if count >= 5:  # Minimum threshold
                        expected = (prod_counts[prod] * line_counts[line]) / total
                        if count > expected * 2:  # At least 2x expected
                            unusual_combos[f"{prod}-Line{line}"] = {
                                "observed": int(count),
                                "expected": float(expected),
                                "ratio": float(count / expected)
                            }
                
                anomaly_data["unusual_product_line_combinations"] = unusual_combos
            
            # Generate prompt for anomaly analysis
            instruction = """
            You are a QA manager in an FMCG factory analyzing anomalies in customer complaints.
            Review the statistical anomalies detected in our customer complaint data.
            For each anomaly:
            1. Explain what makes it significant
            2. Suggest possible root causes
            3. Recommend specific actions for investigation
            
            Focus on being practical and specific with your recommendations.
            """
            
            context = f"""
            Statistical anomalies detected in customer complaint data:
            
            Products with abnormally high defect rates:
            {json.dumps(anomaly_data.get("high_defect_products", {}), indent=2)}
            
            Production lines with unusual defect patterns:
            {json.dumps(anomaly_data.get("high_defect_lines", {}), indent=2)}
            
            Unusual product-line combinations (observed vs expected):
            {json.dumps(anomaly_data.get("unusual_product_line_combinations", {}), indent=2)}
            """
            
            # Generate the analysis
            prompt = self._format_prompt(instruction, context)
            analysis = self._generate_response(prompt)
            
            # Compile results
            results = {
                "anomalies": anomaly_data,
                "ai_analysis": analysis,
                "timestamp": datetime.now().isoformat()
            }
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            return json.dumps({"error": str(e)})
    
    def generate_root_cause_hypotheses(self):
        """
        Generate potential root cause hypotheses for complaint patterns
        
        Returns:
            JSON string with root cause hypotheses
        """
        if self.df.empty:
            return json.dumps({"error": "No complaint data available"})
            
        try:
            # Extract key information for top defects
            top_defects = {}
            
            if 'Tên lỗi' in self.df.columns:
                defect_counts = self.df['Tên lỗi'].value_counts().head(3)
                
                for defect, count in defect_counts.items():
                    defect_data = self.df[self.df['Tên lỗi'] == defect]
                    
                    # Get associated products
                    if 'Tên sản phẩm' in defect_data.columns:
                        products = defect_data['Tên sản phẩm'].value_counts().head(3).to_dict()
                    else:
                        products = {}
                    
                    # Get associated lines
                    if 'Line' in defect_data.columns:
                        lines = defect_data['Line'].value_counts().head(3).to_dict()
                    else:
                        lines = {}
                    
                    # Get associated machines
                    if 'Máy' in defect_data.columns:
                        machines = defect_data['Máy'].value_counts().head(3).to_dict()
                    else:
                        machines = {}
                    
                    # Get QA personnel
                    if 'QA' in defect_data.columns:
                        qa_personnel = defect_data['QA'].value_counts().head(3).to_dict()
                    else:
                        qa_personnel = {}
                    
                    top_defects[defect] = {
                        "count": int(count),
                        "percentage": float(count / len(self.df) * 100),
                        "associated_products": {str(k): int(v) for k, v in products.items()},
                        "associated_lines": {str(k): int(v) for k, v in lines.items()},
                        "associated_machines": {str(k): int(v) for k, v in machines.items()},
                        "associated_qa": {str(k): int(v) for k, v in qa_personnel.items()}
                    }
            
            # Generate prompt for root cause analysis
            instruction = """
            You are a quality assurance expert in an FMCG factory. Based on the customer complaint data provided,
            generate 3-5 plausible root cause hypotheses for the top defects.
            
            For each hypothesis:
            1. Provide a clear, specific title
            2. Explain the potential causal mechanism in detail
            3. Suggest what evidence would support or refute this hypothesis
            4. Recommend specific tests or investigations to validate it
            
            Be specific to the FMCG manufacturing context and consider material, process, equipment, and human factors.
            """
            
            context = f"""
            Customer complaint data for top defects:
            
            {json.dumps(top_defects, indent=2)}
            
            Total complaints analyzed: {len(self.df)}
            """
            
            # Generate the analysis
            prompt = self._format_prompt(instruction, context)
            analysis = self._generate_response(prompt, max_new_tokens=1500)
            
            # Compile results
            results = {
                "top_defects": top_defects,
                "root_cause_hypotheses": analysis,
                "timestamp": datetime.now().isoformat()
            }
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error generating hypotheses: {str(e)}")
            return json.dumps({"error": str(e)})
    
    def recommend_sampling_plan(self):
        """
        Recommend a QA sampling plan based on complaint patterns
        
        Returns:
            JSON string with recommended sampling plan
        """
        if self.df.empty:
            return json.dumps({"error": "No complaint data available"})
            
        try:
            # Prepare sampling context
            sampling_context = {}
            
            # Get products with highest complaint rates
            if 'Tên sản phẩm' in self.df.columns:
                product_complaints = self.df.groupby('Tên sản phẩm').size()
                top_products = product_complaints.nlargest(5).to_dict()
                sampling_context["top_complaint_products"] = {
                    str(k): int(v) for k, v in top_products.items()
                }
            
            # Get lines with highest complaint rates
            if 'Line' in self.df.columns:
                line_complaints = self.df.groupby('Line').size()
                top_lines = line_complaints.nlargest(3).to_dict()
                sampling_context["top_complaint_lines"] = {
                    str(k): int(v) for k, v in top_lines.items()
                }
            
            # Get product-line combinations with high complaint rates
            if all(col in self.df.columns for col in ['Tên sản phẩm', 'Line']):
                prod_line_complaints = self.df.groupby(['Tên sản phẩm', 'Line']).size()
                top_combinations = prod_line_complaints.nlargest(5).to_dict()
                sampling_context["top_product_line_combinations"] = {
                    f"{prod}-Line{line}": int(count) 
                    for (prod, line), count in top_combinations.items()
                }
            
            # Generate prompt for sampling plan
            instruction = """
            You are a QA sampling expert in an FMCG factory. Based on the customer complaint data provided,
            recommend a strategic QA sampling plan that prioritizes resources effectively.
            
            Your sampling plan should include:
            1. Priority products and production lines that need increased sampling frequency
            2. Specific defect types to focus on during inspections
            3. Recommended sampling frequency adjustments with specific numbers
            4. Special monitoring requirements or parameters
            5. Clear sampling methodology improvements
            
            Make your recommendations specific, practical, and actionable for FMCG production.
            """
            
            context = f"""
            Customer complaint analysis for sampling prioritization:
            
            Products with highest complaint counts:
            {json.dumps(sampling_context.get("top_complaint_products", {}), indent=2)}
            
            Production lines with highest complaint counts:
            {json.dumps(sampling_context.get("top_complaint_lines", {}), indent=2)}
            
            Product-line combinations with highest complaints:
            {json.dumps(sampling_context.get("top_product_line_combinations", {}), indent=2)}
            
            Total complaints in dataset: {len(self.df)}
            """
            
            # Generate the sampling plan
            prompt = self._format_prompt(instruction, context)
            sampling_plan = self._generate_response(prompt, max_new_tokens=1500)
            
            # Compile results
            results = {
                "sampling_context": sampling_context,
                "recommended_sampling_plan": sampling_plan,
                "timestamp": datetime.now().isoformat()
            }
            
            return json.dumps(results, indent=2)
            
        except Exception as e:
            logger.error(f"Error recommending sampling plan: {str(e)}")
            return json.dumps({"error": str(e)})
    
    def create_knowledge_base(self, df):
        """
        Create a knowledge base from a DataFrame of complaints
        
        Args:
            df: DataFrame containing complaint data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Fix problematic columns before creating knowledge base
            df_copy = df.copy()
            
            # Fix specific problematic columns
            if "Số lượng (ly/hộp/chai/gói/hủ)" in df_copy.columns:
                df_copy["Số lượng (ly/hộp/chai/gói/hủ)"] = pd.to_numeric(
                    df_copy["Số lượng (ly/hộp/chai/gói/hủ)"].replace('', np.nan), 
                    errors='coerce'
                ).fillna(0)
            
            if "SL pack/ cây lỗi" in df_copy.columns:
                df_copy["SL pack/ cây lỗi"] = pd.to_numeric(
                    df_copy["SL pack/ cây lỗi"].replace('', np.nan), 
                    errors='coerce'
                ).fillna(0)
            
            # Create structured knowledge base
            complaints_data = df_copy.to_dict('records')
            
            knowledge_base = {
                "complaints": complaints_data,
                "metadata": {
                    "last_updated": datetime.now().isoformat(),
                    "total_complaints": len(complaints_data),
                    "date_range": {
                        "start": str(df_copy['Ngày SX'].min()) if "Ngày SX" in df_copy.columns else None,
                        "end": str(df_copy['Ngày SX'].max()) if "Ngày SX" in df_copy.columns else None
                    },
                    "products": df_copy['Tên sản phẩm'].unique().tolist() if "Tên sản phẩm" in df_copy.columns else [],
                    "defect_types": df_copy['Tên lỗi'].unique().tolist() if "Tên lỗi" in df_copy.columns else [],
                    "lines": df_copy['Line'].unique().tolist() if "Line" in df_copy.columns else []
                }
            }
            
            # Save knowledge base to file
            with open(self.knowledge_base_path, 'w') as f:
                json.dump(knowledge_base, f)
                
            # Update instance variables
            self.knowledge_base = knowledge_base
            self.df = df_copy
            
            logger.info(f"Knowledge base created with {len(df_copy)} complaints")
            return True
            
        except Exception as e:
            logger.error(f"Error creating knowledge base: {str(e)}")
            return False

# Example usage
if __name__ == "__main__":
    token = "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"  # Replace with your actual token
    agent = HuggingFaceComplaintAgent(token=token)
    
    # Example analysis
    patterns = agent.identify_patterns(days_back=30)
    print(patterns)

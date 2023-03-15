from pypasser import reCaptchaV3

reCaptcha_response = reCaptchaV3(
    "https://www.google.com/recaptcha/api2/anchor?ar=1&k=6LfwuyUTAAAAAOAmoS0fdqijC2PbbdH4kjq62Y1b&co=aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbTo0NDM.&hl=en&v=MuIyr8Ej74CrXhJDQy37RPBe&size=normal&s=EkPBz-SEE7P0snqVNoZobty7BzjJoW72VQPl9A8eNJfoJ5PSQMvyTsLb3TRC-2t_MPlpL06s0HOaUL2IZTLHNpoOSBo3eSwAVQ84UdqW5FT4XQGKWWooX3vzEmq2j6oYN5WOrRngOXNzi5ZBes31yIHG_Ol45XS73XRmgH-lhKnfgzA0F_ofJbd5lhE-8eoaDXfzvkIidd0rDhDjk1JmVf0T-yFhXqlWB9R7YBQ3y-kiGzzyKRdCKi0PXt-mIX-jBlFO6emVEH3Oqu3apDIm31wD2VogIyQ&cb=z42yo9kwobb8"
)
print(reCaptcha_response)
## use this response in your request ...
